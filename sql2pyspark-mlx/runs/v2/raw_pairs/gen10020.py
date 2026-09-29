from pyspark.sql import functions as F

base = df_shipments.filter(F.col("status") != "IN_TRANSIT")

summary = (
    base
    .groupBy(
        F.when(F.col("cost") >= 97, "high").when(F.col("cost") >= 32, "mid").otherwise("low").alias("cost_bucket"),
        F.quarter("delivered_date").alias("delivered_quarter"),
    )
    .agg(
        F.min("cost").alias("min_cost"),
        F.count("*").alias("row_count"),
    )
)

result = summary
