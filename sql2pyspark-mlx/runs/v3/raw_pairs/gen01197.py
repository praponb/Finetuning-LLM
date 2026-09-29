from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .select(
        F.col("ss.id"),
        F.col("ss.sale_date"),
        F.col("p.stock_qty"),
        F.col("p.cost"),
        F.col("s.store_name"),
        F.add_months(F.col("p.launch_date"), 1).alias("launch_date_plus_months"),
    )
    .orderBy(F.col("id").desc())
)
