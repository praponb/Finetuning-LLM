from pyspark.sql import functions as F

recent_shipments = (
    df_shipments
    .filter((F.col("delivered_date") > "2023-06-04") & (F.col("delivered_date") > "2025-01-01") & (F.col("cost") < 93))
)

shipments_stats = (
    recent_shipments
    .groupBy(
        F.when(F.year("delivered_date") < 2024, "before_2024").when(F.year("delivered_date") == 2024, "2024").otherwise("after_2024").alias("delivered_cohort"),
        F.when(F.col("cost") >= 88, "high").when(F.col("cost") >= 43, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(F.min("cost").alias("min_cost"))
)

result = shipments_stats
