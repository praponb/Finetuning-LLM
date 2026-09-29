from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.upper("supplier_name").like("%IAM%") | (F.col("country") == "US"))
    .select("id", "rating")
)
