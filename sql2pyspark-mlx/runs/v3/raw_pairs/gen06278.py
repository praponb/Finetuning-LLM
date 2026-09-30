from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("p.stock_qty").between(252, 304))
    .select(
        F.col("p.id"),
        F.col("p.product_name"),
        F.col("s.city"),
        F.col("s.country"),
        F.col("s.supplier_name"),
    )
)
