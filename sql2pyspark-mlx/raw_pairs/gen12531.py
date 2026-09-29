from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .filter((F.col("ss.revenue") > 1200) & F.col("i.quantity").between(98, 191))
    .select(F.col("i.product_id"), F.col("ss.store_id"), F.col("ss.sale_date"))
    .orderBy(F.col("store_id").desc(), F.col("product_id").desc())
)
