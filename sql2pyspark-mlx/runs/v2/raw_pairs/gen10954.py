from pyspark.sql import functions as F

recent_shipments = (
    df_shipments
    .filter(F.col("carrier").isin(["Flash", "UPS"]))
    .withColumn("cost_whole", F.ceil("cost"))
)

status_summary = (
    recent_shipments
    .groupBy("status")
    .agg(F.count(F.when(F.col("status") == "IN_TRANSIT", 1)).alias("in_transit_count"))
)

result = (
    status_summary
    .select("status", "in_transit_count")
    .orderBy("status")
)
