from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .filter(F.col("p.supplier_id").isin([6, 7, 15, 21]))
    .select(
        F.col("ss.id"),
        F.col("ss.store_id"),
        F.col("p.stock_qty"),
        F.col("p.launch_date"),
        F.col("s.city"),
        F.col("s.opened_date"),
        (F.col("ss.units") + F.col("ss.revenue")).alias("revenue_per_unit"),
    )
    .orderBy("revenue_per_unit", F.col("stock_qty").desc())
)
