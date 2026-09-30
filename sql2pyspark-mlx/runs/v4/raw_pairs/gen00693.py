from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .filter(F.col("ss.store_id").isin([1, 2]))
    .select(
        F.col("i.id"),
        F.col("i.store_id"),
        F.col("ss.sale_date"),
        F.col("ss.product_id"),
        F.col("ss.units"),
    )
)
