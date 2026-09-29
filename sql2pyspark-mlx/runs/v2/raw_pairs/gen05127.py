from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount") >= 640)
    .groupBy("status")
    .agg(
        F.sum(F.when(F.col("channel") == "web", 1).otherwise(0)).alias("web_count"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
    .orderBy("status")
)
