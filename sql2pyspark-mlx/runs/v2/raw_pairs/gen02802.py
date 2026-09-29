from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee") > 32)
    .select("id", "channel", "order_date")
    .intersect(
        df_orders
        .filter(F.col("amount") < 440)
        .select("id", "channel", "order_date")
    )
    .orderBy("channel")
)
