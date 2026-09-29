from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .filter(F.col("p.stock_qty") >= 297)
    .select(F.col("p.supplier_id"), F.col("s.supplier_name"))
)
