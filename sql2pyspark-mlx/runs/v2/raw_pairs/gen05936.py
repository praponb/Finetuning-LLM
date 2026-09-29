from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("order_id") == 7) & (F.col("amount") >= 430) & (F.col("amount") < 1340))
    .groupBy("method")
    .agg(
        F.count("amount").alias("amount_count"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
    )
    .orderBy(F.col("amount_count").desc(), "method")
    .limit(50)
)
