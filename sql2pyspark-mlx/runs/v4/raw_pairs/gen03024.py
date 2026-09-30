from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "outer")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("p.stock_qty") != 351)
    .groupBy(F.col("p.category"))
    .agg(
        F.sum(F.col("ss.revenue")).alias("total_revenue"),
        F.max(F.col("ss.sale_date")).alias("last_sale_date"),
    )
    .filter(F.col("total_revenue") > 22000)
    .orderBy("total_revenue")
)
