from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount").between(3000, 3300) & (F.col("merchant") == "Grab"))
    .groupBy("is_flagged", "merchant")
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.count("amount").alias("amount_count"),
    )
    .filter(F.col("avg_amount") < 42)
    .orderBy(F.col("avg_amount").desc())
)
