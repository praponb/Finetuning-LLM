from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .filter(F.col("ss.sale_date").between("2025-01-20", "2025-04-08") & (F.col("p.price") > 1180))
    .select(
        F.col("ss.id"),
        F.col("ss.units"),
        F.col("p.price"),
        F.col("p.brand"),
        F.col("s.city"),
        F.col("s.store_name"),
        F.datediff(F.current_date(), F.col("s.opened_date")).alias("days_since_opened_date"),
    )
)
