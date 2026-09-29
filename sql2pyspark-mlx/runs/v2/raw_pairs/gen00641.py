from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date").between("2023-04-13", "2023-12-28"))
    .groupBy(F.quarter("shipped_date").alias("shipped_quarter"))
    .agg(
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
        F.min("cost").alias("min_cost"),
    )
    .filter(F.col("min_cost") >= 56)
)
