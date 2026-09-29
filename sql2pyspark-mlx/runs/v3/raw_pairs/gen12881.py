from pyspark.sql import functions as F

result = (
    df_orders
    .select(
        "id",
        "store_id",
        "customer_id",
        F.date_sub("order_date", 42).alias("order_date_minus_42d"),
    )
)
