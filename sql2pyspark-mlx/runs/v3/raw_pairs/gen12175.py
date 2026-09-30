from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("cost").isNull())
    .agg(
        F.min("shipped_date").alias("min_shipped_date"),
        F.count("*").alias("total_count"),
        F.round(F.avg("cost"), 2).alias("avg_cost"),
    )
    .orderBy(F.col("min_shipped_date").desc())
)
