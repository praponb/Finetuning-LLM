from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time_period", "id")
w2 = Window.partitionBy(F.year("txn_time_period"), "account_id").orderBy("txn_time_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_transactions
    .filter(F.col("merchant").isNotNull() & (F.col("amount") >= 3100))
    .withColumn("txn_time_period", F.date_trunc("MONTH", "txn_time"))
    .withColumn("txn_time_day", F.dayofmonth("txn_time"))
)

with_dims = (
    filtered
    .alias("src")
    .join(df_accounts.alias("b"), F.col("src.account_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.status"))
)

valid = (
    with_dims
    .filter(F.col("account_id").isin([4, 14, 21]))
    .select("*", F.col("is_flagged").cast("int").alias("is_flagged_flag"))
)

scored = (
    valid
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "merchant",
        "is_flagged",
        "txn_time_period",
        "txn_time_day",
        "status",
        "is_flagged_flag",
        F.row_number().over(w).alias("row_seq"),
        F.last("amount", ignorenulls=True).over(w2).alias("last_known_amount"),
    )
)

result = (
    scored
    .groupBy(F.date_trunc("month", "txn_time_period").alias("activity_month"))
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.round(F.sum("row_seq") / F.nullif(F.count(F.when(F.col("txn_type") == "FEE", 1)), F.lit(0)), 2).alias("row_seq_per_fee"),
    )
    .orderBy(F.col("row_seq_per_fee").desc())
)
