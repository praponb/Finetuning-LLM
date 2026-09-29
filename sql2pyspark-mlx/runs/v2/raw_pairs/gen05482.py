from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .select(
        F.col("ss.sale_date"),
        F.col("ss.store_id"),
        F.col("p.supplier_id"),
        F.col("s.store_name"),
    )
    .orderBy("store_id", "store_name")
)
