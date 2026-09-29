from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("%Traders%") & (F.length("supplier_name") >= 8))
    .select("id", "city", "country")
    .orderBy("id")
)
