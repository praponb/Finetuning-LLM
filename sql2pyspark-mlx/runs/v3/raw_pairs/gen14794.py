from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .filter(F.col("ss.sale_date") < "2023-10-01")
    .groupBy(F.col("ss.product_id"))
    .agg(
        (F.max(F.col("i.quantity")) - F.min(F.col("i.quantity"))).alias("quantity_range"),
        F.count(F.col("i.quantity")).alias("quantity_count"),
    )
    .filter(F.col("quantity_count") > 5)
    .orderBy(F.col("quantity_range").desc())
)
