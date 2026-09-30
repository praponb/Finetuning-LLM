from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .groupBy(F.year(F.col("s.opened_date")).alias("opened_year"))
    .agg(F.max(F.col("ss.sale_date")).alias("last_sale_date"))
    .orderBy("last_sale_date")
)
