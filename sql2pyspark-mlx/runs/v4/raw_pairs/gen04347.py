from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .filter(F.col("ss.revenue").between(1700, 2500))
    .select(
        F.col("i.id"),
        F.col("i.product_id"),
        F.col("ss.revenue"),
        F.col("ss.product_id").alias("store_sale_product_id"),
        F.col("ss.store_id"),
    )
    .orderBy(F.col("product_id").desc(), "store_id")
)
