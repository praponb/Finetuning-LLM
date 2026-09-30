from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("carrier")
    .agg(
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
        F.countDistinct("cost").alias("unique_orders"),
    )
    .filter(F.col("stddev_cost") > 47)
    .orderBy("carrier", "unique_orders")
)
