from pyspark.sql import functions as F

base = (
    df_shipments
    .filter(F.col("shipped_date") >= "2024-12-15")
    .withColumn("adjusted_cost", F.round((F.col("cost") * 0.85), 2))
)

shipments_stats = (
    base
    .groupBy(
        "carrier",
        F.when(F.col("adjusted_cost") >= 84, "high").when(F.col("adjusted_cost") >= 10, "mid").otherwise("low").alias("adjusted_cost_bucket"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
)

result = (
    shipments_stats
    .select("carrier", "adjusted_cost_bucket")
    .orderBy("adjusted_cost_bucket")
)
