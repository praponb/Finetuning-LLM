from pyspark.sql import functions as F

base = df_customers.filter((F.col("country") == "SG") & (F.col("city") != "Chiang Mai"))

segment_summary = (
    base
    .groupBy("segment")
    .agg(F.avg("age").alias("avg_age"))
)

result = (
    segment_summary
    .filter(F.col("avg_age") < 36)
    .select("segment", "avg_age")
    .orderBy(F.col("avg_age").desc())
)
