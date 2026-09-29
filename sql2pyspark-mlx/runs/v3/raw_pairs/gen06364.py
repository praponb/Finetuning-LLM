from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(
        F.when(F.col("cost") >= 91, "high").when(F.col("cost") >= 31, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(
        F.min("cost").alias("min_cost"),
        F.countDistinct("cost").alias("unique_statuss"),
    )
    .filter(F.col("unique_statuss") > 3)
    .orderBy(F.col("min_cost").desc(), "cost_bucket")
    .limit(50)
)
