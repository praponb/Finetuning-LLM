from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "outer")
    .select(
        F.col("ss.store_id"),
        F.col("ss.product_id"),
        F.col("s.region"),
        F.col("p.stock_qty"),
        F.col("p.product_name"),
        F.last_day(F.col("ss.sale_date")).alias("sale_date_month_end"),
    )
    .orderBy("product_name")
)
