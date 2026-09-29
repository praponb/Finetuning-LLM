from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .filter(F.col("ss.units").isNotNull())
    .select(
        F.col("i.id"),
        F.col("i.last_restock"),
        F.col("ss.units"),
        (F.col("ss.units") * F.col("ss.revenue")).alias("revenue_per_unit"),
    )
    .orderBy("id", "revenue_per_unit")
)
