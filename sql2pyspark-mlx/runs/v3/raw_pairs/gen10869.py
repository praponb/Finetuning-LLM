from pyspark.sql import functions as F

recent_shipments = (
    df_shipments
    .filter(F.col("carrier").isin(["Flash", "UPS"]))
    .withColumn("cost_whole", F.ceil("cost"))
)

summary = (
    recent_shipments
    .groupBy("order_id")
    .agg(F.min("shipped_date").alias("min_shipped_date"))
)

result = summary.select("order_id", "min_shipped_date")
