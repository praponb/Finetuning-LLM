from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(
        F.when(F.col("cost") >= 107, "high").when(F.col("cost") >= 49, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(
        F.avg("cost").alias("avg_cost"),
        F.sort_array(F.collect_set("carrier")).alias("carrier_list"),
    )
    .orderBy("cost_bucket")
)
