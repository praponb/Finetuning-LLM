from pyspark.sql import functions as F

recent_suppliers = df_suppliers.filter(F.col("country").isin(["JP", "DE", "US"]))

city_summary = (
    recent_suppliers
    .groupBy("city")
    .agg(F.count("rating").alias("rating_count"))
)

result = city_summary.filter(F.col("rating_count") < 10)
