from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(
        F.when(F.col("amount") >= 4000, "HIGH").otherwise("LOW").alias("amount_tier"),
        F.date_trunc("quarter", "txn_time").alias("quarter_start"),
        "txn_type",
    )
    .agg(
        F.round(F.sum("amount") / F.nullif(F.count(F.when(F.col("merchant") == "Amazon", 1)), F.lit(0)), 2).alias("amount_per_amazon"),
        F.size(F.collect_set("merchant")).alias("num_merchants"),
    )
    .orderBy(F.col("amount_per_amazon").desc())
)
