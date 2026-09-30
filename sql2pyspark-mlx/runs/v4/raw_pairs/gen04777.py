from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("discount") <= 0.24) | F.col("discount").isNotNull())
    .groupBy("channel")
    .agg(
        F.max("order_date").alias("last_order_date"),
        F.sum(F.when(F.col("channel") == "partner", 1).otherwise(0)).alias("partner_count"),
    )
    .filter(F.col("partner_count") >= 42)
)
