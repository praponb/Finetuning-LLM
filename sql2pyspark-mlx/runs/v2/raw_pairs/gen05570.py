from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .filter(F.col("ss.store_id").isin([1, 2, 4, 6]))
    .groupBy(
        F.col("ss.product_id"),
        F.date_trunc("MONTH", F.col("ss.sale_date")).alias("sale_month"),
    )
    .agg(
        (F.max(F.col("i.quantity")) - F.min(F.col("i.quantity"))).alias("quantity_range"),
        F.max(F.col("i.quantity")).alias("max_quantity"),
    )
    .filter(F.col("quantity_range") >= 29)
    .orderBy(F.col("quantity_range").desc(), "product_id", "sale_month")
    .limit(1)
)
