from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .select(
        F.col("ss.units"),
        F.col("p.launch_date"),
        F.col("p.stock_qty"),
        F.col("s.city"),
    )
    .orderBy(F.col("city").desc(), F.col("launch_date").desc())
)
