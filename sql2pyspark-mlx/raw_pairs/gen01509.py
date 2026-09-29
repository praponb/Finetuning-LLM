from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .filter(F.year(F.col("i.last_restock")) == 2025)
    .groupBy(F.col("ss.product_id"))
    .agg(
        F.round(F.avg(F.col("i.quantity")), 2).alias("avg_quantity"),
        F.sum(F.col("i.quantity")).alias("total_quantity"),
    )
    .filter(F.col("total_quantity") > 380)
    .orderBy(F.col("total_quantity").desc())
)
