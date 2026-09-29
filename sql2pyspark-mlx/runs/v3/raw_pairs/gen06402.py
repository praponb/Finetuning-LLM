from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id").isin([14, 21, 36]))
    .groupBy("store_id", F.date_trunc("MONTH", "order_date").alias("order_month"))
    .agg(
        F.count("*").alias("row_count"),
        F.count("discount").alias("discount_count"),
    )
    .orderBy("order_month", F.col("discount_count").desc())
)
