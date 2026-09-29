from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("order_id")
    .agg(
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
        F.avg("cost").alias("avg_cost"),
    )
    .filter(F.col("stddev_cost") >= 45)
    .orderBy(F.col("stddev_cost").desc())
)
