from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("T%") & F.col("supplier_name").isNotNull())
    .select("id", "country", "rating", "supplier_name")
)
