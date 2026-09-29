from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(
        F.when(F.col("cost") >= 87, "high").when(F.col("cost") >= 37, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(
        F.avg("cost").alias("avg_cost"),
        F.sum(F.when(F.col("status") == "IN_TRANSIT", F.col("cost")).otherwise(0)).alias("in_transit_cost"),
    )
    .filter(F.col("in_transit_cost") > 70)
    .select("cost_bucket", "avg_cost", "in_transit_cost")
)
