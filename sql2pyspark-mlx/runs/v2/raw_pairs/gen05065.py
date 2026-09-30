from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount") > 720)
    .groupBy("status", "channel")
    .agg(
        F.count("*").alias("num_orders"),
        F.count(F.when(F.col("channel") == "web", 1)).alias("web_count"),
    )
    .orderBy(F.col("status").desc())
)
