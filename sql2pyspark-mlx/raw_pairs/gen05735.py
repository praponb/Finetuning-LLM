from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "outer")
    .filter(F.col("p.supplier_id").isin([16, 25]) | F.col("p.category").isin(["Sports", "Fashion"]))
    .select(F.col("ss.units"), F.col("p.cost"), F.col("s.city"), F.col("s.country"))
    .orderBy("cost")
)
