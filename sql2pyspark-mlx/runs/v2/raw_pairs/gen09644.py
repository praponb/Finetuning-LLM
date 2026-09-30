from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .filter((F.col("p.category") != "Sports") & (F.col("p.supplier_id") == 16))
    .select(
        F.col("ss.id"),
        F.col("ss.revenue"),
        F.col("p.supplier_id"),
        F.col("p.category"),
        F.col("p.price"),
    )
    .orderBy("supplier_id")
)
