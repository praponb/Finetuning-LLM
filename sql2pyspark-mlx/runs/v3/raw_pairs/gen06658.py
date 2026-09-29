from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status") == "LOST")
    .groupBy("carrier")
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
    )
    .filter(F.col("cnt") > 3)
)
