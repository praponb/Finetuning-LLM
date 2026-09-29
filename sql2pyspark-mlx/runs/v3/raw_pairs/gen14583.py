from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.month("order_date").isin([4, 8, 12]) & (F.col("channel") == "partner"))
    .select(
        "channel",
        "store_id",
        "customer_id",
        F.concat("channel", F.lit("/"), "status").alias("channel_status"),
    )
    .orderBy(F.col("customer_id").desc())
)
