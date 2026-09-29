from pyspark.sql import functions as F

recent_suppliers = (
    df_suppliers
    .filter(F.col("rating").isNotNull() & (F.col("country") != "JP"))
    .withColumn("rating_rounded", F.round("rating", 0))
)

city_summary = (
    recent_suppliers
    .groupBy("city")
    .agg(F.count(F.when(F.col("city") == "Bangkok", 1)).alias("bangkok_count"))
)

result = (
    city_summary
    .select("city", "bangkok_count")
    .orderBy("bangkok_count")
)
