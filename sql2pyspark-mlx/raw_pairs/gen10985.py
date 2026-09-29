from pyspark.sql import functions as F

result = (
    df_orders
    .select(
        "id",
        "discount",
        "amount",
        F.concat("status", F.lit("/"), "channel").alias("status_channel"),
    )
)
