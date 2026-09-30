from pyspark.sql import functions as F

src = (
    df_orders
    .filter(F.col("amount") >= 1000)
    .withColumn("status_length", F.length("status"))
)

status_summary = (
    src
    .groupBy("status", "customer_id")
    .agg(F.sort_array(F.collect_set("status")).alias("status_list"))
)

result = status_summary.orderBy(F.col("customer_id").desc())
