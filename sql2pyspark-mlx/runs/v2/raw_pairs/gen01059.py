from pyspark.sql import functions as F

base = (
    df_shipments
    .filter(F.month("delivered_date").isin([2, 8, 12]) & (F.col("order_id") == 3))
    .withColumn("delivered_date_month", F.month("delivered_date"))
)

shipments_stats = (
    base
    .groupBy("carrier", "status")
    .agg(F.max("cost").alias("max_cost"))
)

result = shipments_stats.select("carrier", "status", "max_cost")
