from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "left")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .filter(F.col("p.product_name").isNotNull())
    .groupBy(F.month(F.col("s.opened_date")).alias("opened_month"), F.col("s.city"))
    .agg(F.countDistinct(F.col("ss.units")).alias("unique_stores"))
    .filter(F.col("unique_stores") > 3)
)
