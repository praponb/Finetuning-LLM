from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .select(
        F.col("ss.revenue"),
        F.col("ss.sale_date"),
        F.col("p.brand"),
        F.col("p.product_name"),
        F.col("s.country"),
        F.round((F.col("p.cost") * 1.15), 2).alias("cost_adjusted"),
    )
)
