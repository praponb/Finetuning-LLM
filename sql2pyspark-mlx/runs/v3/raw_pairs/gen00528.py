from pyspark.sql import functions as F

recent_shipments = (
    df_shipments
    .filter(F.col("order_id").isin([15, 22, 25, 28]))
    .withColumn("cost_whole", F.ceil("cost"))
)

carrier_summary = (
    recent_shipments
    .groupBy("carrier")
    .agg(F.count("*").alias("num_shipments"))
)

result = (
    carrier_summary
    .filter(F.col("num_shipments") > 10)
    .select("carrier", "num_shipments")
    .orderBy("carrier", F.col("num_shipments").desc())
    .limit(5)
)
