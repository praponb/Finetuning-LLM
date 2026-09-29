from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .filter(F.col("ss.product_id").isin([2, 5, 6, 8]) | (F.col("s.rating") <= 3.0))
    .select(
        F.col("ss.id"),
        F.col("ss.revenue"),
        F.col("p.stock_qty"),
        F.col("s.country"),
        F.col("s.rating"),
    )
)
