from pyspark.sql import functions as F

result = (
    df_suppliers
    .select("id", "city", F.trim("supplier_name").alias("clean_supplier_name"))
    .orderBy("city", "clean_supplier_name", "id")
    .limit(50)
)
