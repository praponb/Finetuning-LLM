from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .select(
        F.col("ss.id"),
        F.col("ss.product_id"),
        F.col("ss.sale_date"),
        F.col("p.cost"),
        F.col("s.country"),
        F.round((F.col("ss.revenue") / F.col("ss.units")), 2).alias("revenue_per_unit"),
    )
    .orderBy("country")
)
