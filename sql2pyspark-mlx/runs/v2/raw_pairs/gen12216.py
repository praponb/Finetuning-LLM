from pyspark.sql import functions as F

result = (
    df_shipments
    .agg(
        F.sum(F.when(F.col("status") == "IN_TRANSIT", 1).otherwise(0)).alias("in_transit_count"),
        F.min("delivered_date").alias("min_delivered_date"),
        F.round(F.avg("cost"), 2).alias("avg_cost"),
    )
    .orderBy(F.col("avg_cost").desc())
)
