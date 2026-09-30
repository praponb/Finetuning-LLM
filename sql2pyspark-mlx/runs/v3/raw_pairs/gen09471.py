from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("p.category").isin(["Sports", "Electronics"]))
    .select(
        F.col("ss.id"),
        F.col("ss.revenue"),
        F.col("ss.sale_date"),
        F.col("p.supplier_id"),
        F.col("p.brand"),
        F.col("s.city"),
        F.col("s.supplier_name"),
        F.length(F.col("s.country")).alias("country_length"),
    )
    .orderBy(F.col("id").desc())
)
