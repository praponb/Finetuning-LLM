from pyspark.sql import functions as F

recent_shipments = (
    df_shipments
    .filter((F.col("order_id").isin([28, 39]) & (F.year("delivered_date") >= 2024)) | (F.col("carrier") == "FedEx"))
    .withColumn("cost_rounded", F.round("cost", 0))
)

agg = (
    recent_shipments
    .groupBy(
        "carrier",
        F.when(F.col("cost") >= 102, "high").when(F.col("cost") >= 21, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(
        F.min("cost").alias("min_cost"),
        F.min("cost_rounded").alias("min_cost_rounded"),
    )
)

result = (
    agg
    .filter(F.col("min_cost_rounded") >= 75)
    .select("carrier", "cost_bucket", "min_cost", "min_cost_rounded")
)
