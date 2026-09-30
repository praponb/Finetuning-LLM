from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .select(
        F.col("ss.id"),
        F.col("ss.revenue"),
        F.col("ss.units"),
        F.col("p.brand"),
        F.col("p.category"),
        F.col("s.opened_date"),
        F.col("s.store_name"),
        (F.col("ss.units") + F.col("ss.revenue")).alias("revenue_per_unit"),
    )
    .orderBy(F.col("revenue_per_unit").desc(), F.col("opened_date").desc())
)
