from pyspark.sql import functions as F

result = (
    df_suppliers
    .select("id", "supplier_name", "city", F.length("country").alias("country_length"))
    .orderBy("country_length", "supplier_name")
)
