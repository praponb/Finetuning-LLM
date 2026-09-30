from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .filter(F.col("s.store_name").isNotNull() & F.col("ss.sale_date").between("2023-11-01", "2023-12-01"))
    .select(
        F.col("ss.id"),
        F.col("ss.units"),
        F.col("p.product_name"),
        F.col("p.price"),
        F.col("s.opened_date"),
    )
)
