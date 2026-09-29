from pyspark.sql import functions as F

result = (
    df_suppliers
    .select("id", "supplier_name", "city", F.lower("country").alias("country_lower"))
)
