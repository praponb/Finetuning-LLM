from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("p.category").isin(["Home", "Books", "Fashion"]))
    .select(
        F.col("ss.id"),
        F.col("ss.units"),
        F.col("ss.product_id"),
        F.col("p.brand"),
        F.col("s.supplier_name"),
        F.col("s.city"),
    )
    .orderBy(F.col("brand").desc(), "city", "id")
    .limit(10)
)
