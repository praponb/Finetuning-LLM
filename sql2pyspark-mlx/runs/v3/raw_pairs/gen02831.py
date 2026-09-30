from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .filter(F.array_contains(F.col("p.tags"), "bestseller") & (F.col("p.stock_qty") == 373))
    .select(
        F.col("s.rating"),
        F.col("s.city"),
        F.col("s.supplier_name"),
        F.col("p.price"),
        F.col("p.category"),
        F.length(F.col("p.category")).alias("category_length"),
    )
)
