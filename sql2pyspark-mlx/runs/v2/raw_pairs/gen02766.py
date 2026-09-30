from pyspark.sql import functions as F

result = (
    df_orders
    .agg(
        F.max("order_date").alias("last_order_date"),
        F.count("discount").alias("discount_count"),
    )
)
