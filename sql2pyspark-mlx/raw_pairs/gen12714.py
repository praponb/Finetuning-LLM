from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .filter(~F.col("p.product_name").like("Yog%") & F.col("s.region").isin(["South", "Central", "North"]))
    .select(
        F.col("ss.id"),
        F.col("ss.product_id"),
        F.col("p.stock_qty"),
        F.col("s.store_name"),
        F.col("s.opened_date"),
        F.floor(F.col("p.price")).alias("price_whole"),
    )
    .orderBy(F.col("price_whole").desc(), "id")
    .limit(10)
)
