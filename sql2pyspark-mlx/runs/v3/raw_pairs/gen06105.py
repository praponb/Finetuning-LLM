from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id").isin([7, 18, 21]))
    .groupBy(F.month("order_date").alias("order_month"), "status")
    .agg(
        F.countDistinct("shipping_fee").alias("unique_statuss"),
        F.sum(F.when(F.col("status") == "SHIPPED", 1).otherwise(0)).alias("shipped_count"),
    )
    .filter(F.col("unique_statuss") >= 5)
)
