from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .filter((F.col("s.rating") > 4.2) & (F.col("p.price") > 1180))
    .select(
        F.col("p.product_name"),
        F.col("p.launch_date"),
        F.col("p.stock_qty"),
        F.col("s.country"),
        F.trim(F.col("p.category")).alias("clean_category"),
    )
    .orderBy("country")
)
