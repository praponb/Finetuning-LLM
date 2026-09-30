from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(
        F.when(F.year("order_date") < 2024, "before_2024").when(F.year("order_date") == 2024, "2024").otherwise("after_2024").alias("order_cohort"),
    )
    .agg(
        F.avg("discount").alias("avg_discount"),
        F.count(F.when(F.col("status") == "SHIPPED", 1)).alias("shipped_count"),
        F.sum(F.when(F.col("status") == "SHIPPED", F.col("discount")).otherwise(0)).alias("shipped_discount"),
    )
    .filter(F.col("shipped_discount") >= 29)
)
