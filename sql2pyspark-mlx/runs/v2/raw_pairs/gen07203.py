from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("status")
    .agg(
        (F.max("cost") - F.min("cost")).alias("cost_range"),
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
    )
)
