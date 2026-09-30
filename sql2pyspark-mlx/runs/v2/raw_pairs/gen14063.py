from pyspark.sql import functions as F

result = (
    df_orders
    .select(
        "id",
        "shipping_fee",
        F.date_add("order_date", 62).alias("order_date_plus_62d"),
    )
)
