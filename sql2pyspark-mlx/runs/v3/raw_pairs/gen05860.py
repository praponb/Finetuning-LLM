from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .filter(F.col("p.product_name").like("%Lapto%"))
    .select(
        F.col("ss.units"),
        F.col("p.supplier_id"),
        F.col("p.launch_date"),
        F.col("s.store_name"),
        F.col("s.opened_date"),
    )
    .orderBy(F.col("units").desc())
)
