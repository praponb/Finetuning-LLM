from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .filter(F.col("ss.product_id") == 6)
    .groupBy(F.col("s.city"))
    .agg(
        F.max(F.col("ss.units")).alias("max_units"),
        F.max(F.col("ss.sale_date")).alias("last_sale_date"),
    )
)
