from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status")
    .agg(
        F.max("order_date").alias("last_order_date"),
        F.count("discount").alias("discount_count"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
    )
)
