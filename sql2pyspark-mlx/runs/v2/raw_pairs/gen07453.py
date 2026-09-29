from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("delivered_date") < "2025-08-01") & (F.col("delivered_date") < "2024-03-10") & (F.year("delivered_date") >= 2025))
    .groupBy("status")
    .agg(
        F.min("cost").alias("min_cost"),
        F.min("delivered_date").alias("min_delivered_date"),
        F.round(F.avg("cost"), 2).alias("avg_cost"),
    )
    .orderBy("status")
    .limit(20)
)
