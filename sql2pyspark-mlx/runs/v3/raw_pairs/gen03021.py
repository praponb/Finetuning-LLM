from pyspark.sql import functions as F

result = (
    df_orders
    .select(
        "channel",
        "amount",
        "discount",
        F.date_format("order_date", "yyyy-MM-dd").alias("order_date_label"),
    )
)
