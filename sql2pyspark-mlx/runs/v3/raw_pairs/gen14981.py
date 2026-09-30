from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .filter(F.col("i.quantity") > 103)
    .groupBy(
        F.quarter(F.col("ss.sale_date")).alias("sale_quarter"),
        F.date_format(F.col("ss.sale_date"), "yyyy-MM").alias("sale_month"),
    )
    .agg(F.avg(F.col("i.quantity")).alias("avg_quantity"))
)
