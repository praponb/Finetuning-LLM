from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .filter(F.col("ss.store_id").isin([1, 5, 6]) | ~F.col("p.category").isin(["Grocery", "Books", "Home"]))
    .select(
        F.col("ss.id"),
        F.col("ss.store_id"),
        F.col("ss.product_id"),
        F.col("p.product_name"),
        F.col("p.supplier_id"),
        F.col("s.region"),
        F.col("s.store_name"),
    )
)
