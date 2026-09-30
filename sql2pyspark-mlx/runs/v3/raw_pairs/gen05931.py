from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.year("delivered_date") >= 2025)
    .groupBy("carrier")
    .agg(
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
        F.min("cost").alias("min_cost"),
    )
    .filter(F.col("stddev_cost") < 47)
    .orderBy("min_cost")
)
