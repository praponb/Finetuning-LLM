from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "right")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .filter((F.year(F.col("s.opened_date")) >= 2025) | F.col("ss.product_id").isin([4, 5]))
    .select(
        F.col("ss.id"),
        F.col("ss.units"),
        F.col("ss.store_id"),
        F.col("p.product_name"),
        F.col("p.supplier_id"),
        F.col("s.city"),
        F.col("s.store_name"),
    )
    .orderBy(F.col("id").desc())
)
