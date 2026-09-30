from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter((F.col("p.stock_qty") < 211) & (F.col("s.city") == "Bangkok"))
    .select(
        F.col("ss.units"),
        F.col("ss.store_id"),
        F.col("p.stock_qty"),
        F.col("p.category"),
        F.col("s.country"),
        F.col("s.supplier_name"),
    )
)
