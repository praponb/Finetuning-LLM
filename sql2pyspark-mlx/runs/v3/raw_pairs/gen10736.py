from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel")
    .agg(F.count("*").alias("num_orders"))
    .filter(F.col("num_orders") >= 4)
    .select("channel", "num_orders")
    .orderBy(F.col("num_orders").desc(), "channel")
)
