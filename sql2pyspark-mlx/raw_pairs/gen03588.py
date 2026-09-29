from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .select(
        F.col("i.store_id"),
        F.col("ss.store_id").alias("store_sale_store_id"),
        F.col("ss.sale_date"),
        F.col("ss.product_id"),
        F.round(F.col("i.quantity"), 2).alias("quantity_rounded"),
    )
    .orderBy("quantity_rounded", F.col("product_id").desc())
)
