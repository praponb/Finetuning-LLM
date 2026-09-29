from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(
        "status",
        F.when(F.col("cost") >= 98, "high").when(F.col("cost") >= 37, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(
        F.count("cost").alias("cost_count"),
        F.countDistinct("cost").alias("unique_carriers"),
    )
    .filter(F.col("cost_count") >= 3)
    .select("status", "cost_bucket", "cost_count", "unique_carriers")
    .orderBy("cost_bucket", "cost_count")
)
