from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .filter((F.col("ss.revenue") < 3000) | F.col("ss.units").between(23, 29))
    .select(
        F.col("i.store_id"),
        F.col("i.last_restock"),
        F.col("ss.units"),
        F.col("ss.sale_date"),
        F.col("ss.store_id").alias("store_sale_store_id"),
        F.round(F.col("i.quantity"), 0).alias("quantity_rounded"),
    )
)
