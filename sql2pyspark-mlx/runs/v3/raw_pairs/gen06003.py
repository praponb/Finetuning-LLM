from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "outer")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .filter(F.col("s.country").isin(["TH", "JP"]))
    .select(
        F.col("ss.id"),
        F.col("ss.store_id"),
        F.col("p.brand"),
        F.col("s.rating"),
        F.lower(F.col("s.country")).alias("country_lower"),
    )
)
