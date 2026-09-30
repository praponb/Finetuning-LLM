from pyspark.sql import functions as F

result = (
    df_orders
    .agg(
        F.count("amount").alias("amount_count"),
        F.max("order_date").alias("latest_order_date"),
    )
)
