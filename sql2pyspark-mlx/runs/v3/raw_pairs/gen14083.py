from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("cost") > 80)
    .groupBy("carrier")
    .agg(
        F.min("shipped_date").alias("first_shipped_date"),
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
    )
    .filter(F.col("stddev_cost") > 63)
)
