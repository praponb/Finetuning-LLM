from pyspark.sql import functions as F

valid_shipments = df_shipments.filter(F.col("delivered_date") < "2023-05-01")

shipments_stats = (
    valid_shipments
    .groupBy(
        F.when(F.col("cost") >= 86, "high").when(F.col("cost") >= 33, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(F.round(F.stddev("cost"), 2).alias("stddev_cost"))
)

result = shipments_stats
