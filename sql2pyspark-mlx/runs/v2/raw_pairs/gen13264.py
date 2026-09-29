from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id").isin([5, 6, 40]))
    .groupBy(F.year("order_date").alias("order_year"))
    .agg(F.avg("discount").alias("avg_discount"))
    .filter(F.col("avg_discount") >= 0.3)
    .orderBy(F.col("avg_discount").desc(), "order_year")
    .limit(5)
)
