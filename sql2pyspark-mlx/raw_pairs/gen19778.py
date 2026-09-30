from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .filter(F.col("ss.product_id").isin([1, 5, 8]))
    .select(
        F.col("i.store_id"),
        F.col("ss.product_id"),
        F.col("ss.sale_date"),
        F.col("ss.revenue"),
    )
    .orderBy(F.col("product_id").desc(), F.col("revenue").desc())
)
