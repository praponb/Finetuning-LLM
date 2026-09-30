from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .filter(F.col("p.brand") != "Globex")
    .groupBy(F.col("s.country"))
    .agg(
        F.sum(F.col("ss.revenue")).alias("total_revenue"),
        F.count(F.col("ss.units")).alias("units_count"),
    )
)
