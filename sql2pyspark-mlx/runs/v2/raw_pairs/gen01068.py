from pyspark.sql import functions as F

recent_shipments = df_shipments.filter(F.col("carrier") != "Kerry")

shipments_stats = (
    recent_shipments
    .groupBy("status")
    .agg(
        F.min("cost").alias("min_cost"),
        F.max("cost").alias("max_cost"),
    )
)

result = shipments_stats.filter(F.col("max_cost") < 63)
