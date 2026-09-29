from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id") == 31)
    .select(
        "order_date",
        "channel",
        F.initcap(F.trim("channel")).alias("channel_title"),
    )
    .orderBy("order_date")
)
