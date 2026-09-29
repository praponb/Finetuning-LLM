from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("ss.product_id").isin([1, 5, 8]))
    .select(
        F.col("ss.id"),
        F.col("ss.revenue"),
        F.col("ss.store_id"),
        F.col("p.product_name"),
        F.col("s.supplier_name"),
        (F.col("p.stock_qty") + F.col("p.cost")).alias("stock_qty_plus_cost"),
    )
)
