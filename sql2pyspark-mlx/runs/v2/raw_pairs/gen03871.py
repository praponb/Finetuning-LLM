from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("country") == "DE") | (F.col("country") == "JP"))
    .select("id", "supplier_name", "rating", F.lower("country").alias("country_lower"))
)
