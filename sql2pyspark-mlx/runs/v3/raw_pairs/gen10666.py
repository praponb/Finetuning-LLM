from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("s.rating").isNotNull())
    .select(
        F.col("ss.id"),
        F.col("ss.store_id"),
        F.col("p.stock_qty"),
        F.col("s.city"),
        F.col("s.supplier_name"),
    )
    .orderBy("supplier_name")
)
