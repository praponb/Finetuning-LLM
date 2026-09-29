from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.month("delivered_date").isin([7, 11, 12]))
    .groupBy(
        "status",
        F.when(F.col("cost") >= 89, "high").when(F.col("cost") >= 33, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(
        F.min("shipped_date").alias("first_shipped_date"),
        F.countDistinct("cost").alias("unique_carriers"),
    )
    .filter(F.col("unique_carriers") >= 5)
)
