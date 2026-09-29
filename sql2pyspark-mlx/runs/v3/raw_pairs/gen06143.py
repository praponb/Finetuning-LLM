from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter((F.col("p.category") == "Books") & (F.col("p.stock_qty") == 372))
    .select(
        F.col("ss.store_id"),
        F.col("p.supplier_id"),
        F.col("p.launch_date"),
        F.col("s.rating"),
    )
)
