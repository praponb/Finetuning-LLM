from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .select(
        F.col("ss.store_id"),
        F.col("ss.units"),
        F.col("p.cost"),
        F.col("p.category"),
        F.col("s.region"),
        F.col("s.opened_date"),
    )
    .orderBy(F.col("units").desc())
)
