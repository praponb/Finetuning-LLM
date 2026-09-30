from pyspark.sql import functions as F

result = (
    df_shipments
    .withColumn("cost_tier", F.when(F.col("cost") >= 96, "PREMIUM").otherwise("STANDARD"))
    .cube("status", "cost_tier")
    .agg(
        F.avg(F.when(F.col("status") == "DELIVERED", F.col("cost"))).alias("delivered_avg_cost"),
        F.count_if(F.col("status") == "RETURNED").alias("returned_events"),
        F.countDistinct("id").alias("distinct_ids"),
        F.grouping_id().alias("grouping_id"),
    )
    .filter(F.col("delivered_avg_cost") >= 0)
    .orderBy(F.col("returned_events").desc())
)
