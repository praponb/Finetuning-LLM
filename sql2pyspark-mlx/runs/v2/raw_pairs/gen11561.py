from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("%City%") & (F.col("rating") > 3.9) & (F.col("city") == "Chiang Mai"))
    .select("country", "city", "supplier_name")
)
