from pyspark.sql import functions as F

result = (
    df_orders
    .agg(
        F.max("amount").alias("max_amount"),
        F.min("order_date").alias("first_order_date"),
    )
)
