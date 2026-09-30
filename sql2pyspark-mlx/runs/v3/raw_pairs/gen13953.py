from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("ss.units").between(15, 24))
    .groupBy(F.col("s.city"))
    .agg(
        F.countDistinct(F.col("ss.revenue")).alias("unique_products"),
        F.round(F.avg(F.col("ss.revenue")), 2).alias("avg_revenue"),
    )
)
