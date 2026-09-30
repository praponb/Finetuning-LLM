from pyspark.sql import functions as F

base = df_shipments.filter((F.col("cost") <= 34) | (F.col("cost") < 92))

shipments_stats = (
    base
    .groupBy("carrier")
    .agg(F.min("shipped_date").alias("min_shipped_date"))
)

result = (
    shipments_stats
    .select("carrier", "min_shipped_date")
    .orderBy("carrier")
    .limit(1)
)
