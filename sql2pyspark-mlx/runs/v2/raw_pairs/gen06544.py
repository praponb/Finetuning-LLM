from pyspark.sql import functions as F

base = (
    df_shipments
    .filter(F.col("order_id").isin([8, 25]) & F.col("order_id").isin([23, 25, 26]) & F.col("status").isin(["LOST", "RETURNED", "IN_TRANSIT"]))
    .withColumn("shipped_date_period", F.date_trunc("week", "shipped_date"))
)

shipments_stats = (
    base
    .groupBy("status")
    .agg(F.min("cost").alias("min_cost"))
)

result = (
    shipments_stats
    .filter(F.col("min_cost") > 27)
    .select("status", "min_cost")
    .orderBy("status", F.col("min_cost").desc())
)
