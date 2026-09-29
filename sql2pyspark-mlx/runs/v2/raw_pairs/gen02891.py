from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .filter((F.col("ss.units") != 23) & (F.col("i.quantity") >= 128))
    .select(
        F.col("i.product_id"),
        F.col("i.last_restock"),
        F.col("i.store_id"),
        F.col("ss.sale_date"),
        F.when(F.col("ss.units") >= 45, "High").when(F.col("ss.units") >= 10, "Medium").otherwise("Low").alias("units_tier"),
    )
    .orderBy(F.col("last_restock").desc())
)
