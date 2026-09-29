from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .filter(~(F.col("i.product_id").isin([1, 3, 9, 10]) | (F.col("i.last_restock") >= "2025-10-14")))
    .select(
        F.col("i.id"),
        F.col("i.store_id"),
        F.col("i.product_id"),
        F.col("ss.units"),
        F.col("ss.product_id").alias("store_sale_product_id"),
        F.col("ss.revenue"),
        F.round((F.col("ss.units") / F.col("ss.revenue")), 2).alias("revenue_per_unit"),
    )
)
