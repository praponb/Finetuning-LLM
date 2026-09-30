from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.filter(F.col("stock_qty").between(252, 344)).alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left_semi")
    .filter(F.length(F.col("s.supplier_name")) >= 6)
    .select(F.col("s.id"), F.col("s.supplier_name"))
    .orderBy("supplier_name")
)
