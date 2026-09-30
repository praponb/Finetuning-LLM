from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee") < 10)
    .select("shipping_fee", F.coalesce("channel", F.lit("N/A")).alias("channel_filled"))
    .orderBy(F.col("shipping_fee").desc())
)
