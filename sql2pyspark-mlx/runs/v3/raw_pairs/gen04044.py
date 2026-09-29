from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.filter(F.col("supplier_name").like("Si%")).alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left_semi")
    .filter(F.col("p.supplier_id").isin([9, 13, 26]))
    .select(F.col("p.id"), F.col("p.brand"), F.col("p.product_name"), F.col("p.price"))
)
