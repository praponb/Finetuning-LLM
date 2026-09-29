from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .select(
        F.col("i.id"),
        F.col("i.store_id"),
        F.col("i.quantity"),
        F.col("i.last_restock"),
        F.col("ss.units"),
        F.date_format(F.col("i.last_restock"), "yyyy-MM").alias("last_restock_label"),
    )
    .orderBy(F.col("last_restock").desc())
)
