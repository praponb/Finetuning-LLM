from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.month("delivered_date").isin([5, 6, 8]))
    .groupBy("carrier", "status")
    .agg(
        (F.max("cost") - F.min("cost")).alias("cost_range"),
        F.count("cost").alias("cost_count"),
    )
    .filter(F.col("cost_count") > 1)
)
