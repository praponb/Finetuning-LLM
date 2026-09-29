from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .select(
        F.col("i.id"),
        F.col("i.store_id"),
        F.col("i.last_restock"),
        F.col("ss.units"),
        F.date_add(F.col("i.last_restock"), 7).alias("last_restock_plus_7d"),
    )
    .orderBy(F.col("id").desc())
)
