from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .filter(F.col("i.last_restock") > "2025-01-02")
    .select(
        F.col("i.id"),
        F.col("i.last_restock"),
        F.col("i.store_id"),
        F.col("i.product_id"),
        F.col("ss.store_id").alias("store_sale_store_id"),
        F.col("ss.revenue"),
        F.add_months(F.col("i.last_restock"), 12).alias("last_restock_plus_months"),
    )
    .orderBy("store_sale_store_id", "id")
)
