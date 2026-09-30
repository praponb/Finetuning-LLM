from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(
        F.when(F.col("channel").isin(["store", "web"]), "core").otherwise("other").alias("channel_group"),
    )
    .agg(F.count("*").alias("total_count"))
)
