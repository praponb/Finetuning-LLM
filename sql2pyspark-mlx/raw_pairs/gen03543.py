from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .filter((F.col("ss.units") != 39) & F.col("i.product_id").isin([1, 2, 4, 9]))
    .select(
        F.col("i.id"),
        F.col("i.product_id"),
        F.col("i.store_id"),
        F.col("ss.units"),
    )
)
