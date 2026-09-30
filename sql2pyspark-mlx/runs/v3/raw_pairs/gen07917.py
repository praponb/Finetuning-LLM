from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("status") != "COMPLETED")
    .select(
        "id",
        "discount",
        "amount",
        F.trim("channel").alias("clean_channel"),
        F.concat("channel", F.lit("/"), "status").alias("channel_status"),
    )
    .orderBy("id")
)
