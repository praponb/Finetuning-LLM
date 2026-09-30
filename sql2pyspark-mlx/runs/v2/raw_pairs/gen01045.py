from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .filter(F.col("i.quantity") > 118)
    .groupBy(F.col("ss.store_id"))
    .agg(
        F.max(F.col("i.quantity")).alias("max_quantity"),
        F.min(F.col("i.quantity")).alias("min_quantity"),
    )
    .orderBy(F.col("min_quantity").desc())
)
