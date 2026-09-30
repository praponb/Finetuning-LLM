from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country") == "DE")
    .select("id", "supplier_name", "city", "rating")
)
