from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("country") == "SG") & ~F.col("supplier_name").like("Si%") & (F.col("country") == "SG"))
    .select("supplier_name")
)
