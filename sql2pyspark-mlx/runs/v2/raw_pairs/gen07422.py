from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("status")
    .agg(
        F.min("cost").alias("min_cost"),
        F.sum(F.when(F.col("status") == "LOST", 1).otherwise(0)).alias("lost_count"),
    )
)
