from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(
        F.when(F.col("cost") >= 87, "high").when(F.col("cost") >= 40, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(F.max("shipped_date").alias("latest_shipped_date"))
)
