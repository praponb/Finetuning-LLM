from pyspark.sql import functions as F

filtered_shipments = (
    df_shipments
    .filter(F.col("order_id").isNotNull() | F.col("order_id").isNull())
    .withColumn("cost_rounded", F.round("cost", 1))
)

shipments_stats = (
    filtered_shipments
    .groupBy("carrier")
    .agg(F.sort_array(F.collect_set("status")).alias("status_list"))
)

result = shipments_stats.orderBy("carrier")
