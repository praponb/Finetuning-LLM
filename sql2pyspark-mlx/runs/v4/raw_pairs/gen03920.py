from pyspark.sql import functions as F

base = (
    df_shipments
    .filter(~((F.col("cost") < 35) | F.col("cost").between(38, 59)))
    .withColumn("delivered_date_dayofweek", F.dayofweek("delivered_date"))
)

carrier_summary = (
    base
    .groupBy("carrier", F.month("shipped_date").alias("shipped_month"))
    .agg(
        F.count(F.when(F.col("carrier") == "FedEx", 1)).alias("fedex_count"),
        F.round(F.avg("cost"), 2).alias("avg_cost"),
    )
)

result = carrier_summary.select("carrier", "fedex_count", "avg_cost")
