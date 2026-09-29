from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .select(
        F.col("i.product_id"),
        F.col("ss.revenue"),
        F.col("ss.units"),
        F.round((F.col("i.quantity") * 1.07), 2).alias("quantity_scaled"),
    )
    .orderBy(F.col("revenue").desc())
)
