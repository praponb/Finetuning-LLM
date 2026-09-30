from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .select(
        F.col("i.id"),
        F.col("i.store_id"),
        F.col("i.quantity"),
        F.col("i.product_id"),
        F.col("ss.revenue"),
        F.col("ss.store_id").alias("store_sale_store_id"),
        F.when(F.col("i.quantity") >= 226, "High").when(F.col("i.quantity") >= 90, "Medium").otherwise("Low").alias("quantity_level"),
    )
    .orderBy(F.col("revenue").desc())
)
