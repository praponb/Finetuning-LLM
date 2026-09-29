from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date") <= "2024-04-18")
    .groupBy(
        "carrier",
        F.when(F.col("cost") >= 89, "high").when(F.col("cost") >= 42, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(
        F.min("cost").alias("min_cost"),
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
    )
    .orderBy(F.col("min_cost").desc())
)
