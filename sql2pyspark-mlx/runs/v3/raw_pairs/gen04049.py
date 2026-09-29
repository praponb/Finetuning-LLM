from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount") <= 1560)
    .withColumn("status_channel_key", F.concat_ws("-", "status", "channel"))
    .withColumn("channel_filled", F.coalesce("channel", F.lit("unassigned")))
)
