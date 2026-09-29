from pyspark.sql import functions as F

result = (
    df_orders
    .withColumn("channel_filled", F.coalesce("channel", F.lit("unassigned")))
    .withColumn("shipping_fee_filled", F.coalesce("shipping_fee", F.lit(0)))
)
