from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("rating") <= 1.8) & F.col("supplier_name").like("P%"))
    .select("id", "country", "rating", F.lower("country").alias("country_lower"))
)
