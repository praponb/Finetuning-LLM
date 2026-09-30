from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("s.country") == "TH")
    .select(F.col("p.price"), F.col("s.city"), F.col("s.supplier_name"))
)
