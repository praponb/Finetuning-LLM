from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "left")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .select(
        F.col("ss.units"),
        F.col("p.supplier_id"),
        F.col("p.price"),
        F.col("s.store_name"),
    )
)
