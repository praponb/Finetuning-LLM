from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country")
    .agg(F.count(F.when(F.col("city") == "Singapore", 1)).alias("singapore_count"))
    .filter(F.col("singapore_count") < 7)
    .select("country", "singapore_count")
    .orderBy(F.col("singapore_count").desc(), "country")
    .limit(10)
)
