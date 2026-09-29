from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel").isin(["web", "mobile"]))
    .groupBy("status")
    .agg(F.sort_array(F.collect_set("status")).alias("status_list"))
    .orderBy(F.col("status").desc())
)
