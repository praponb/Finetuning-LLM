from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(-3, -1)
w2 = Window.partitionBy("merchant").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("txn_type", "account_id").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w_recent = Window.orderBy("is_flagged_flag", "id")
w_ordered = Window.orderBy("balance", "id")
w7 = Window.partitionBy(F.to_date("txn_time"), "status").orderBy("is_flagged_flag", "id")
w8 = Window.partitionBy("account_type").orderBy("txn_time", "id").rowsBetween(-5, -1)
w9 = Window.orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_transactions
    .withColumn("amount_avg_3_prior", F.avg("amount").over(w))
    .withColumn("running_amount", F.sum("amount").over(w2))
)

cleaned = (
    scored
    .filter(F.col("txn_time") >= F.to_date(F.lit("2023-09-01")))
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        "running_amount",
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
        F.coalesce("merchant", F.lit("UNKNOWN")).alias("merchant_filled"),
    )
)

enriched = (
    cleaned
    .select(
        "*",
        F.last("merchant", ignorenulls=True).over(w3).alias("last_known_merchant"),
        F.avg("is_flagged_flag").over(w4).alias("is_flagged_flag_avg_7d"),
    )
)

enriched_transactions = (
    enriched
    .alias("e")
    .join(df_accounts.alias("p"), F.col("e.account_id") == F.col("p.id"), "inner")
    .select("e.*", F.col("p.status"), F.col("p.balance"), F.col("p.account_type"))
)

with_dims = (
    enriched_transactions
    .alias("s")
    .join(df_accounts.alias("p"), F.col("s.account_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.opened_date"))
)

with_dims_transactions = (
    with_dims
    .alias("a")
    .join(df_accounts.alias("p2"), F.col("a.account_id") == F.col("p2.id"), "inner")
    .select("a.*", F.col("p2.status").alias("account_status"))
)

with_windows = (
    with_dims_transactions
    .select(
        "id",
        "amount",
        "merchant",
        "is_flagged",
        "is_flagged_flag",
        "last_known_merchant",
        "status",
        "balance",
        F.lag("balance", 1, 0).over(w_recent).alias("prev_balance"),
        F.lag("is_flagged_flag_avg_7d", 1).over(w_ordered).alias("prev_is_flagged_flag_avg_7d"),
        F.row_number().over(w7).alias("seq_no"),
        F.avg("running_amount").over(w8).alias("running_amount_avg_5_prior"),
        F.first("merchant", ignorenulls=True).over(w9).alias("first_known_merchant"),
    )
)

result = (
    with_windows
    .groupBy("status")
    .pivot("merchant", ["Shell", "Grab", "Starbucks"])
    .agg(F.max("amount").alias("total"), F.count(F.lit(1)).alias("item_count"))
)
