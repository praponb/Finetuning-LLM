from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .filter(F.col("s.city") == "Tokyo")
    .groupBy(F.col("p.category"))
    .agg(
        F.max(F.col("ss.sale_date")).alias("last_sale_date"),
        F.count(F.col("ss.units")).alias("units_count"),
    )
    .filter(F.col("units_count") >= 5)
)
