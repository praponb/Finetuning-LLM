from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("order_date") < "2025-03-01")
    .groupBy(F.when(F.col("discount") >= 0.42, "high").when(F.col("discount") >= 0.19, "mid").otherwise("low").alias("discount_bucket"))
    .pivot("status", ["CANCELLED", "SHIPPED", "COMPLETED"])
    .agg(F.sum("amount"))
    .orderBy("discount_bucket")
)
