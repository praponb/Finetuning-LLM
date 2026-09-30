from pyspark.sql import functions as F

result = (
    df_orders
    .select("id", "channel", F.substring("status", 1, 2).alias("status_prefix"))
    .orderBy(F.col("id").desc(), "channel")
)
