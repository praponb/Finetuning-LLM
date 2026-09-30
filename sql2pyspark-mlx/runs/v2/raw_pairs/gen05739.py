from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.filter(F.col("stock_qty").between(102, 113)).alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left_semi")
    .filter(F.col("s.supplier_name").like("%Imp%"))
    .select(F.col("s.id"), F.col("s.supplier_name"))
    .orderBy("supplier_name", F.col("id").desc())
)
