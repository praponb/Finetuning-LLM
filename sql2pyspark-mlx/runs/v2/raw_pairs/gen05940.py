from pyspark.sql import functions as F

result = (
    df_orders
    .agg(F.sum(F.when(F.col("channel") == "web", 1).otherwise(0)).alias("web_count"))
)
