from pyspark.sql import functions as F

result = (
    df_orders
    .select(
        "customer_id",
        "amount",
        "discount",
        F.upper("channel").alias("channel_upper"),
    )
    .orderBy(F.col("discount").desc())
)
