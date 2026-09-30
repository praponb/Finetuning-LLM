from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel")
    .agg(
        F.countDistinct("shipping_fee").alias("unique_customers"),
        F.sum(F.when(F.col("status") == "SHIPPED", 1).otherwise(0)).alias("shipped_count"),
    )
    .filter(F.col("shipped_count") >= 69)
    .orderBy(F.col("channel").desc())
)
