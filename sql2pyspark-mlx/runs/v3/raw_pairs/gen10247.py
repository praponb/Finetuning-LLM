from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .select(
        F.col("i.store_id"),
        F.col("i.product_id"),
        F.col("ss.sale_date"),
        F.col("ss.revenue"),
        F.date_trunc("year", F.col("i.last_restock")).alias("last_restock_period"),
    )
    .orderBy(F.col("last_restock_period").desc())
)
