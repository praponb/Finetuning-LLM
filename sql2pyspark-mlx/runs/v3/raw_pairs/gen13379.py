from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status")
    .agg(
        F.count("discount").alias("discount_count"),
        F.max("amount").alias("max_amount"),
    )
    .orderBy(F.col("status").desc())
)
