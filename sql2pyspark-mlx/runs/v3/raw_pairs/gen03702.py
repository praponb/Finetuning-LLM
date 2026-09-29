from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("%GmbH%"))
    .select("country", F.trim("supplier_name").alias("clean_supplier_name"))
)
