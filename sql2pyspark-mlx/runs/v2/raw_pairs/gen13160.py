from pyspark.sql import functions as F

result = (
    df_orders
    .select("discount", "channel", F.col("discount").cast("int").alias("discount_int"))
)
