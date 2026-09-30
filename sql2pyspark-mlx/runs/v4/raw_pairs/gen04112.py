from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(
        F.when(F.col("cost") >= 81, "high").when(F.col("cost") >= 20, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(
        F.countDistinct("cost").alias("unique_orders"),
        F.max("shipped_date").alias("latest_shipped_date"),
    )
    .filter(F.col("unique_orders") < 10)
    .orderBy(F.col("unique_orders").desc(), "cost_bucket")
    .limit(20)
)
