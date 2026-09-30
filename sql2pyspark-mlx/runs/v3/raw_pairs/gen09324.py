from pyspark.sql import functions as F

result = (
    df_suppliers
    .select("id", "city", "country", F.lower("city").alias("city_lower"))
    .orderBy("city")
)
