from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .filter(~((F.col("p.category") == "Books") & (F.col("s.rating") < 2.3)))
    .select(
        F.col("s.rating"),
        F.col("s.supplier_name"),
        F.col("s.city"),
        F.col("p.stock_qty"),
        F.col("p.category"),
    )
    .orderBy("stock_qty")
)
