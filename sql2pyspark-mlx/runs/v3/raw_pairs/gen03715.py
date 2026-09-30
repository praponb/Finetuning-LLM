from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .filter(F.col("ss.product_id").isin([8, 10]))
    .groupBy(F.year(F.col("ss.sale_date")).alias("sale_year"), F.col("ss.product_id"))
    .agg(
        F.sum(F.col("i.quantity")).alias("total_quantity"),
        (F.max(F.col("i.quantity")) - F.min(F.col("i.quantity"))).alias("quantity_range"),
    )
)
