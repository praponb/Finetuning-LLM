from pyspark.sql import functions as F

result = (
    df_orders
    .filter(~F.col("status").isin(["RETURNED", "COMPLETED", "SHIPPED"]))
    .select(
        "id",
        "store_id",
        "shipping_fee",
        "order_date",
        F.concat("channel", F.lit("/"), "status").alias("channel_status"),
        F.coalesce("discount", F.lit(0)).alias("discount_filled"),
    )
    .orderBy(F.col("discount_filled").desc(), "id")
    .limit(1)
)
