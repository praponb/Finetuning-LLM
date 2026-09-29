from pyspark.sql import functions as F

src = (
    df_shipments
    .filter(F.col("order_id").isin([7, 11, 35, 39]))
    .withColumn("delivered_date_period", F.date_trunc("week", "delivered_date"))
)

carrier_summary = (
    src
    .groupBy("carrier")
    .agg(
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.count("cost").alias("cost_count"),
    )
)

result = (
    carrier_summary
    .filter(F.col("cost_count") >= 6)
    .select("carrier", "cost_count")
    .orderBy(F.col("cost_count").desc())
)
