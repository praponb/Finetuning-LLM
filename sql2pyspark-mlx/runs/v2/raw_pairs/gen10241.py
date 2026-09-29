from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .filter(F.col("ss.sale_date") > "2024-11-01")
    .select(
        F.col("ss.id"),
        F.col("ss.product_id"),
        F.col("ss.store_id"),
        F.col("p.cost"),
        F.col("p.launch_date"),
        F.col("s.rating"),
        F.lower(F.col("s.country")).alias("country_lower"),
    )
)
