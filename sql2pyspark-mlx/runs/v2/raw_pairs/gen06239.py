from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier").isin(["Kerry", "DHL"]))
    .groupBy("status")
    .agg(F.round(F.stddev("cost"), 2).alias("stddev_cost"))
)
