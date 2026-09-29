from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .filter(F.col("ss.store_id").isin([1, 3, 5]) & F.col("ss.store_id").isin([1, 2, 3, 6]))
    .select(
        F.col("i.store_id"),
        F.col("ss.sale_date"),
        F.col("ss.product_id"),
        F.col("ss.units"),
        F.round((F.col("i.quantity") * 1.07), 2).alias("adjusted_quantity"),
    )
    .orderBy("product_id", F.col("store_id").desc())
)
