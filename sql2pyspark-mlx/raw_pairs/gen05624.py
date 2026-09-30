from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id").isin([3, 34]))
    .select("status", F.coalesce("channel", F.lit("N/A")).alias("channel_filled"))
)
