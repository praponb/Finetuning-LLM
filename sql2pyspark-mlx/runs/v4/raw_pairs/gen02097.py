from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .filter(F.col("ss.product_id").isin([2, 3, 7]))
    .groupBy(
        F.col("ss.product_id"),
        F.quarter(F.col("ss.sale_date")).alias("sale_quarter"),
    )
    .agg(
        F.min(F.col("i.last_restock")).alias("min_last_restock"),
        F.count(F.col("i.quantity")).alias("quantity_count"),
    )
    .orderBy(F.col("sale_quarter").desc(), "product_id")
    .limit(20)
)
