from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status") == "RETURNED")
    .groupBy("status")
    .agg(
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
        (F.max("cost") - F.min("cost")).alias("cost_range"),
    )
)
