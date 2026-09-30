from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .groupBy(F.col("ss.product_id"), F.year(F.col("ss.sale_date")).alias("sale_year"))
    .agg(F.countDistinct(F.col("i.quantity")).alias("unique_stores"))
    .filter(F.col("unique_stores") >= 5)
    .orderBy(F.col("product_id").desc())
)
