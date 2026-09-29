from pyspark.sql import functions as F

src = df_transactions.filter(F.col("txn_time") >= "2023-11-01")

is_flagged_summary = (
    src
    .groupBy("is_flagged", "txn_type")
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.sum(F.when(F.col("merchant") == "Lazada", 1).otherwise(0)).alias("lazada_count"),
    )
)

result = is_flagged_summary.select("is_flagged", "txn_type", "avg_amount", "lazada_count")
