from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city") == "London")
    .select("id", "rating", "supplier_name")
)
