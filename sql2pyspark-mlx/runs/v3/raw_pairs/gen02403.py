from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .groupBy(F.col("p.category"))
    .agg(
        F.min(F.col("ss.units")).alias("min_units"),
        F.avg(F.col("ss.revenue")).alias("avg_revenue"),
    )
    .filter(F.col("avg_revenue") >= 3800)
)
