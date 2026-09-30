from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .select(
        F.col("i.id"),
        F.col("i.last_restock"),
        F.col("ss.units"),
        F.col("ss.revenue"),
        F.round((F.col("ss.revenue") / F.col("ss.units")), 2).alias("revenue_per_unit"),
    )
    .orderBy(F.col("revenue").desc())
)
