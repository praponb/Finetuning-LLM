from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(
        F.when(F.col("channel").isin(["partner", "web"]), "priority").otherwise("other").alias("channel_group"),
    )
    .agg(F.max("order_date").alias("latest_order_date"))
)
