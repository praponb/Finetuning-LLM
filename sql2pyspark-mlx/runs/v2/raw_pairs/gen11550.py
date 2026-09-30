from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "right")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .select(
        F.col("ss.id"),
        F.col("ss.units"),
        F.col("p.category"),
        F.col("p.cost"),
        F.col("s.opened_date"),
    )
)
