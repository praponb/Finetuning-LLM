from pyspark.sql import functions as F

result = (
    df_suppliers
    .select("supplier_name", F.trim("country").alias("clean_country"))
    .orderBy("supplier_name")
)
