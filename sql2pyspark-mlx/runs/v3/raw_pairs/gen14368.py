from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .filter(F.col("i.store_id").isin([2, 4, 5, 6]))
    .groupBy(
        F.col("ss.store_id"),
        F.quarter(F.col("ss.sale_date")).alias("sale_quarter"),
    )
    .agg(
        F.sum(F.col("i.quantity")).alias("total_quantity"),
        (F.max(F.col("i.quantity")) - F.min(F.col("i.quantity"))).alias("quantity_range"),
    )
    .orderBy(F.col("store_id").desc(), F.col("total_quantity").desc())
)
