from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(
        F.quarter("shipped_date").alias("shipped_quarter"),
        F.when(F.col("cost") >= 105, "high").when(F.col("cost") >= 43, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(
        F.count(F.when(F.col("carrier") == "Kerry", 1)).alias("kerry_count"),
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
    )
    .filter(F.col("stddev_cost") > 26)
    .orderBy(F.col("stddev_cost").desc())
)
