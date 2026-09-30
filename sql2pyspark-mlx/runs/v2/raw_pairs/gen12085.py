from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "right")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .filter((F.col("ss.sale_date") >= "2024-05-01") & F.col("ss.units").between(17, 30))
    .select(
        F.col("ss.product_id"),
        F.col("s.city"),
        F.col("s.store_name"),
        F.col("p.category"),
    )
    .orderBy(F.col("city").desc())
)
