from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .filter(F.col("ss.sale_date") < "2025-06-01")
    .groupBy(
        F.col("ss.product_id"),
        F.date_format(F.col("ss.sale_date"), "yyyy-MM").alias("sale_month"),
    )
    .agg(F.sum(F.col("i.quantity")).alias("total_quantity"))
    .filter(F.col("total_quantity") >= 910)
)
