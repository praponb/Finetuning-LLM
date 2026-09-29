from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .filter((F.col("s.rating") < 3.5) | (F.col("p.cost") >= 510))
    .select(F.col("s.city"), F.col("p.stock_qty"), F.col("p.product_name"))
    .orderBy("product_name", "city")
)
