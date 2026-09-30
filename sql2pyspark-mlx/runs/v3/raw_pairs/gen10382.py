from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.lower("supplier_name").like("%aci%") & F.col("supplier_name").like("Eur%") & (F.col("rating") >= 3.7))
    .select("rating")
)
