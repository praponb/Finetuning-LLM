from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("carrier")
    .agg(
        F.count(F.when(F.col("status") == "DELIVERED", 1)).alias("delivered_count"),
        F.max("delivered_date").alias("latest_delivered_date"),
        F.round(F.avg("cost"), 2).alias("avg_cost"),
    )
    .orderBy(F.col("delivered_count").desc())
)
