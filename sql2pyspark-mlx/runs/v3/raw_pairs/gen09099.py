from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") <= 2.4)
    .select("id", "rating", "supplier_name", F.lower("country").alias("country_lower"))
    .orderBy("id")
)
