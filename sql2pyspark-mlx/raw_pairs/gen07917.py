from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(
        F.when(F.col("amount") >= 3100, "HIGH").otherwise("LOW").alias("amount_tier"),
        "txn_type",
    )
    .agg(
        F.percentile_approx("amount", 0.95).alias("amount_p95"),
        F.count_if(F.col("merchant") == "Shell").alias("shell_events"),
        F.percentile_approx("amount", 0.5).alias("amount_p50"),
        F.sum(F.when(F.col("merchant") == "Lazada", F.col("amount")).otherwise(0)).alias("lazada_amount"),
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
    )
)
