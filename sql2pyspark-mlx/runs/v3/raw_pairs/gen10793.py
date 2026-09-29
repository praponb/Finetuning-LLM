from pyspark.sql import functions as F

result = (
    df_suppliers
    .select("city", "country", "supplier_name", F.trim("city").alias("clean_city"))
)
