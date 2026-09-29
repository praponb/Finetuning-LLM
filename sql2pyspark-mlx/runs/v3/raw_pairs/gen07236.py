from pyspark.sql import functions as F

base = (
    df_suppliers
    .filter(F.col("country").isin(["DE", "JP"]) & (F.col("city") == "Chiang Mai"))
)

city_summary = (
    base
    .groupBy("city")
    .agg(F.count("*").alias("total_count"))
)

result = (
    city_summary
    .filter(F.col("total_count") < 5)
    .orderBy("city")
    .limit(5)
)
