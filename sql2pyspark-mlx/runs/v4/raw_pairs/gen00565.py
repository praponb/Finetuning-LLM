from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "outer")
    .filter((F.size(F.col("p.tags")) > 1) & F.col("s.rating").between(1.8, 3.2))
    .select(
        F.col("ss.store_id"),
        F.col("ss.units"),
        F.col("p.cost"),
        F.col("p.stock_qty"),
        F.col("s.supplier_name"),
        F.upper(F.col("s.country")).alias("country_upper"),
    )
)
