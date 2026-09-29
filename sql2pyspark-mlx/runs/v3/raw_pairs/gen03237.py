from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .filter((F.col("i.last_restock") > "2023-12-01") | F.col("i.product_id").isin([1, 3, 6, 9]))
    .select(
        F.col("i.id"),
        F.col("i.last_restock"),
        F.col("ss.units"),
        F.date_sub(F.col("i.last_restock"), 31).alias("last_restock_minus_31d"),
    )
    .orderBy("last_restock")
)
