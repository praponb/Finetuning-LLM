from pyspark.sql import functions as F

base = (
    df_shipments
    .filter((F.col("shipped_date") >= "2024-09-07") & (F.col("shipped_date") >= "2024-09-01"))
)

summary = (
    base
    .groupBy(F.quarter("shipped_date").alias("shipped_quarter"))
    .agg(F.count(F.when(F.col("carrier") == "UPS", 1)).alias("ups_count"))
)

result = (
    summary
    .filter(F.col("ups_count") < 5)
    .select("shipped_quarter", "ups_count")
    .orderBy(F.col("ups_count").desc(), "shipped_quarter")
)
