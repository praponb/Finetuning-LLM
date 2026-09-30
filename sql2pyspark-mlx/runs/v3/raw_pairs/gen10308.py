from pyspark.sql import functions as F

result = (
    df_shipments
    .agg(
        F.count("*").alias("row_count"),
        F.round(F.avg("cost"), 2).alias("avg_cost"),
    )
    .orderBy("avg_cost", F.col("row_count").desc())
)
