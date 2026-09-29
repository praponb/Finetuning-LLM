from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .filter(F.col("ss.sale_date") < "2024-05-01")
    .groupBy(F.col("ss.product_id"), F.year(F.col("ss.sale_date")).alias("sale_year"))
    .agg(F.min(F.col("i.last_restock")).alias("first_last_restock"))
)
