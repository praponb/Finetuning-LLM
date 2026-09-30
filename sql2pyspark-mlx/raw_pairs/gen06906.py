from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy(F.col("amount").desc(), "id")
w2 = Window.partitionBy("amount_tier").orderBy(F.col("shell_events").desc())

deduped = (
    df_transactions
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "account_id", "amount", "txn_time", "merchant")
)

result = (
    deduped
    .groupBy(
        F.when(F.col("amount") >= 4100, "HIGH").otherwise("LOW").alias("amount_tier"),
        "merchant",
    )
    .agg(
        F.count("*").alias("cnt"),
        F.max("txn_time").alias("last_txn_time"),
        F.count_if(F.col("merchant") == "Shell").alias("shell_events"),
        F.percentile_approx("amount", [0.25, 0.5, 0.75]).alias("amount_quartiles"),
        F.count(F.when(F.col("merchant") == "Amazon", 1)).alias("amazon_count"),
        F.percentile_approx("amount", 0.95).alias("amount_p95"),
    )
    .withColumn("shell_events_rank", F.rank().over(w2))
    .orderBy("cnt")
)
