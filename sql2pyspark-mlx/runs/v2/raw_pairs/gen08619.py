from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("order_id", F.quarter("shipped_date").alias("shipped_quarter"))
    .agg(
        (F.max("cost") - F.min("cost")).alias("cost_range"),
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
    )
    .filter(F.col("stddev_cost") < 56)
)
