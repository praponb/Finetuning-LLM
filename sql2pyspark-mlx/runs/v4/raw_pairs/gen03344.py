from pyspark.sql import functions as F

valid_shipments = (
    df_shipments
    .filter((F.col("status") != "DELIVERED") & (F.col("cost").between(26, 47) | F.col("cost").between(30, 76)))
)

cost_bucket_summary = (
    valid_shipments
    .groupBy(
        F.when(F.col("cost") >= 81, "high").when(F.col("cost") >= 38, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(F.min("cost").alias("min_cost"))
)

result = (
    cost_bucket_summary
    .select("cost_bucket", "min_cost")
    .orderBy("min_cost", "cost_bucket")
    .limit(20)
)
