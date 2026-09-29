from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel")
    .agg(
        F.min("order_date").alias("first_order_date"),
        F.count(F.when(F.col("channel") == "web", 1)).alias("web_count"),
    )
)
