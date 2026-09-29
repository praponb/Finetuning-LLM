from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("country") == "US") & (F.col("rating") <= 3.5))
    .select("id", "rating", "country", F.length("country").alias("country_length"))
    .orderBy(F.col("id").desc())
)
