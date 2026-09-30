from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "left")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("ss.units").between(26, 37) & (F.col("p.stock_qty") < 185))
    .select(
        F.col("ss.id"),
        F.col("ss.product_id"),
        F.col("p.stock_qty"),
        F.col("p.product_name"),
        F.col("s.country"),
    )
)
