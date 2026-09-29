from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") <= 3.9)
    .select("id", "country", F.lower("city").alias("city_lower"))
    .orderBy("id")
)
