from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "left")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .filter(F.col("ss.store_id") == 4)
    .groupBy(F.col("s.city"))
    .agg(
        F.count(F.col("ss.units")).alias("units_count"),
        F.avg(F.col("ss.revenue")).alias("avg_revenue"),
    )
)
