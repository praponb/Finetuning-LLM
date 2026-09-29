from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .select(
        F.col("p.id"),
        F.col("p.cost"),
        F.col("p.price"),
        F.col("p.product_name"),
        F.col("s.supplier_name"),
        F.col("s.rating"),
        F.col("s.country"),
    )
)
