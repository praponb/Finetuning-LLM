from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(
        F.when(F.col("cost") >= 92, "high").when(F.col("cost") >= 44, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(
        F.count("cost").alias("cost_count"),
        F.sum("cost").alias("total_cost"),
    )
    .filter(F.col("total_cost") > 290)
    .drop("total_cost")
    .orderBy(F.col("cost_count").desc())
)
