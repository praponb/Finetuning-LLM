from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .select(
        F.col("p.product_name"),
        F.col("p.supplier_id"),
        F.col("s.supplier_name"),
        F.col("s.city"),
        F.col("s.rating"),
        F.floor(F.col("p.price")).alias("price_whole"),
    )
)
