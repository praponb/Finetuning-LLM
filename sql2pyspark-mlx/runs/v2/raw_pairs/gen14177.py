from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(~(F.col("p.product_name").like("Noi%") & F.col("s.supplier_name").like("E%")))
    .select(F.col("ss.product_id"), F.col("p.cost"), F.col("p.price"), F.col("s.city"))
    .orderBy("cost")
)
