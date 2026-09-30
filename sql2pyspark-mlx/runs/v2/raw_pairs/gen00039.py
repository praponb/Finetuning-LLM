from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("status")
    .agg(
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.count("cost").alias("cost_count"),
    )
    .filter(F.col("avg_cost") >= 41)
    .orderBy("avg_cost", F.col("status").desc())
)
