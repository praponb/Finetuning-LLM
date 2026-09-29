from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.month("shipped_date").isin([1, 3, 9]))
    .groupBy("carrier")
    .agg(
        F.min("cost").alias("min_cost"),
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
    )
    .filter(F.col("stddev_cost") > 71)
)
