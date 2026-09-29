from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("status") == "SHIPPED")
    .select(
        "id",
        "customer_id",
        "discount",
        "amount",
        F.concat("channel", F.lit("/"), "status").alias("channel_status"),
    )
)
