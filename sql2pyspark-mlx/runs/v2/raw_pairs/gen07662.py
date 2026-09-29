from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.array_contains(F.col("p.tags"), "sale") & ~F.col("p.product_name").like("%Wat%"))
    .select(
        F.col("s.country"),
        F.col("p.category"),
        F.col("p.price"),
        F.col("p.stock_qty"),
        F.substring(F.col("s.country"), 1, 4).alias("country_prefix"),
    )
)
