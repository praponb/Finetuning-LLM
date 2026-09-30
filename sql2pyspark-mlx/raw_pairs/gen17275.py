from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy(F.col("amount").desc(), "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id")
w3 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy(F.to_date("txn_time"), "account_id").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w5 = Window.partitionBy("txn_type").orderBy("new_group_flag", "id")

latest = (
    df_transactions
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

filtered = (
    latest
    .filter((F.col("txn_time") >= F.lit("2023-04-20").cast("date")) & F.col("merchant").isNotNull())
    .withColumn("is_lazada", F.when(F.col("merchant") == "Lazada", 1).otherwise(0))
    .withColumn("merchant_lower", F.lower("merchant"))
)

with_gaps = (
    filtered
    .withColumn("new_group_flag", F.when(F.lag("txn_time").over(w2).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w2) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("burst_id", F.sum("new_group_flag").over(w3))

with_dims = (
    sessionized
    .alias("a")
    .join(df_accounts.alias("d"), F.col("a.account_id") == F.col("d.id"), "left")
    .select(
        "a.id",
        "a.account_id",
        "a.txn_type",
        "a.txn_time",
        "a.merchant",
        "a.is_flagged",
        "a.is_lazada",
        "a.merchant_lower",
        "a.new_group_flag",
        F.col("d.opened_date"),
    )
)

scored = (
    with_dims
    .withColumn("is_lazada_sum_7d", F.sum("is_lazada").over(w4))
    .withColumn("new_group_flag_bucket", F.ntile(4).over(w5))
)

result = (
    scored
    .alias("e")
    .join(df_accounts.alias("dim"), F.col("e.account_id") == F.col("dim.id"), "inner")
    .groupingSets(
        [["account_type", "txn_type"], ["account_type"]],
        F.col("dim.account_type").alias("account_type"),
        F.col("e.txn_type").alias("txn_type"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.min(F.col("e.new_group_flag_bucket")).alias("min_new_group_flag_bucket"),
        F.grouping_id().alias("level_id"),
    )
)
