from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_transactions
    .filter(F.col("txn_time") >= F.lit("2023-06-05").cast("date"))
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.when(F.col("amount") >= 3600, "High").when(F.col("amount") >= 800, "Medium").otherwise("Low").alias("amount_level"),
        F.coalesce("merchant", F.lit("N/A")).alias("merchant_filled"),
    )
)

with_gaps = (
    filtered
    .withColumn("is_new_session", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessions_transactions = with_gaps.withColumn("session_no", F.sum("is_new_session").over(w2))

result = (
    sessions_transactions
    .groupBy(
        F.when(F.col("session_no") >= 8, "LARGE").otherwise("SMALL").alias("session_no_tier"),
    )
    .agg(
        F.size(F.collect_set("merchant")).alias("num_merchants"),
        F.sum(F.when(F.col("merchant") == "Amazon", F.col("session_no"))).alias("amazon_total_session_no"),
        F.max("txn_time").alias("ended_at"),
    )
    .filter(F.col("amazon_total_session_no") > 0)
    .orderBy(F.col("amazon_total_session_no").desc_nulls_last())
)
