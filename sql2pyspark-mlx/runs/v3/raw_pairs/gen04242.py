from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(~((F.col("status") == "RETURNED") | F.col("cost").isNotNull()))
    .groupBy(
        F.when(F.col("cost") >= 80, "high").when(F.col("cost") >= 44, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(
        F.count(F.when(F.col("status") == "LOST", 1)).alias("lost_count"),
        F.min("cost").alias("min_cost"),
        F.avg("cost").alias("avg_cost"),
    )
    .filter(F.col("avg_cost") > 43)
    .drop("avg_cost")
)
