from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .filter((F.col("ss.units") != 15) & F.col("i.store_id").isin([1, 2]))
    .select(
        F.col("i.id"),
        F.col("i.last_restock"),
        F.col("i.product_id"),
        F.col("ss.store_id"),
        F.least(F.col("ss.revenue"), F.col("ss.units")).alias("least_revenue_units"),
    )
    .orderBy(F.col("store_id").desc())
)
