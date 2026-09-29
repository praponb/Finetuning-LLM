from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.array_contains(F.col("p.tags"), "eco") & F.col("p.supplier_id").isNull())
    .select(
        F.col("ss.store_id"),
        F.col("ss.revenue"),
        F.col("p.brand"),
        F.col("s.rating"),
        F.round(F.col("s.rating"), 2).alias("rating_rounded"),
    )
)
