from pyspark.sql import functions as F

result = (
    df_orders
    .select(
        "id",
        "channel",
        "shipping_fee",
        F.add_months("order_date", 3).alias("order_date_plus_months"),
        F.date_format("order_date", "yyyy-MM").alias("order_date_label"),
    )
)
