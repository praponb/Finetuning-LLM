from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(
        "carrier",
        F.when(F.col("cost") >= 95, "high").when(F.col("cost") >= 30, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
        F.max("cost").alias("max_cost"),
    )
)
