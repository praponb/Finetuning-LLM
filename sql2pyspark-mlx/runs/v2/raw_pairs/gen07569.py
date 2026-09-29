from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("ss.store_id") == 4)
    .groupBy(
        F.col("p.category"),
        F.quarter(F.col("p.launch_date")).alias("launch_quarter"),
    )
    .agg(
        F.count(F.col("ss.revenue")).alias("revenue_count"),
        F.min(F.col("ss.sale_date")).alias("min_sale_date"),
    )
)
