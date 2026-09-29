from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .filter(F.col("i.quantity") <= 218)
    .groupBy(F.year(F.col("ss.sale_date")).alias("sale_year"))
    .agg(
        F.round(F.avg(F.col("i.quantity")), 2).alias("avg_quantity"),
        (F.max(F.col("i.quantity")) - F.min(F.col("i.quantity"))).alias("quantity_range"),
    )
    .orderBy(F.col("quantity_range").desc())
)
