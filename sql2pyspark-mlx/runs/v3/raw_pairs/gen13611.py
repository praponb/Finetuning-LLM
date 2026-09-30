from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.lower("supplier_name").like("%oky%"))
    .select("city", "rating", "country", F.upper("country").alias("country_upper"))
    .orderBy("city")
)
