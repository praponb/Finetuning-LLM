from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(F.date_format("delivered_date", "yyyy-MM").alias("delivered_month"))
    .agg(
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
        F.round(F.avg("cost"), 2).alias("avg_cost"),
    )
    .filter(F.col("stddev_cost") > 62)
)
