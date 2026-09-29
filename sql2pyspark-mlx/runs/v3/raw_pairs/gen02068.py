from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .filter(F.col("ss.revenue").between(1100, 1900))
    .groupBy(F.col("ss.store_id"), F.year(F.col("ss.sale_date")).alias("sale_year"))
    .agg(F.max(F.col("i.last_restock")).alias("last_last_restock"))
    .orderBy(F.col("sale_year").desc(), "store_id")
    .limit(5)
)
