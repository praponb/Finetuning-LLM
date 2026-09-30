from pyspark.sql import functions as F

filtered_suppliers = df_suppliers.filter(F.col("country").isin(["DE", "SG", "TH"]))

city_summary = (
    filtered_suppliers
    .groupBy("city")
    .agg(F.count(F.when(F.col("city") == "Singapore", 1)).alias("singapore_count"))
)

result = (
    city_summary
    .filter(F.col("singapore_count") > 4)
    .orderBy("singapore_count")
)
