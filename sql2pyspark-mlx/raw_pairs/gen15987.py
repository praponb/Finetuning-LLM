from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.lower(F.trim("supplier_name")) == "siam parts co") & (F.col("country") != "JP"))
    .groupBy("city")
    .agg(F.count(F.when(F.col("city") == "Singapore", 1)).alias("singapore_count"))
    .orderBy("singapore_count", "city")
    .limit(5)
)
