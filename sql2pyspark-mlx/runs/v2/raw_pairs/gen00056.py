from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") < 4.1)
    .select("id", "country", "rating", "city", F.length("city").alias("city_length"))
)
