from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .select(
        F.col("ss.store_id"),
        F.col("ss.product_id"),
        F.col("s.store_name"),
        F.col("s.opened_date"),
        F.col("p.product_name"),
        F.dayofweek(F.col("s.opened_date")).alias("opened_date_dayofweek"),
    )
)
