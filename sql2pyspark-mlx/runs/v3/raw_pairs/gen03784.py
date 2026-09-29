from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .filter(F.col("i.quantity").between(74, 142))
    .groupBy(F.col("ss.product_id"))
    .agg(
        F.count(F.col("i.quantity")).alias("quantity_count"),
        F.avg(F.col("i.quantity")).alias("avg_quantity"),
    )
    .orderBy(F.col("avg_quantity").desc())
)
