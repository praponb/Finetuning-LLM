from pyspark.sql import functions as F

valid_shipments = df_shipments.filter((F.col("cost") >= 93) & (F.col("cost") > 94))

agg = (
    valid_shipments
    .groupBy(
        F.when(F.col("cost") >= 90, "high").when(F.col("cost") >= 49, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(F.max("delivered_date").alias("latest_delivered_date"))
)

result = (
    agg
    .orderBy("cost_bucket")
    .limit(1)
)
