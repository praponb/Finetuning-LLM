from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "right")
    .filter(F.col("s.city").isNotNull())
    .select(
        F.col("ss.store_id"),
        F.col("ss.revenue"),
        F.col("p.stock_qty"),
        F.col("p.category"),
        F.col("s.country"),
        F.col("s.city"),
        F.col("s.rating").cast("int").alias("rating_int"),
    )
    .orderBy(F.col("stock_qty").desc())
)
