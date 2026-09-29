from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .groupBy(F.col("s.city"))
    .agg(
        F.max(F.col("ss.revenue")).alias("max_revenue"),
        F.max(F.col("ss.units")).alias("max_units"),
    )
    .filter(F.col("max_units") > 37)
)
