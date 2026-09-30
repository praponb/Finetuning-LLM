from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .filter(F.col("s.store_name").like("Sia%") & (F.col("p.cost") < 530))
    .select(
        F.col("ss.units"),
        F.col("ss.product_id"),
        F.col("p.stock_qty"),
        F.col("s.opened_date"),
    )
    .orderBy("opened_date")
)
