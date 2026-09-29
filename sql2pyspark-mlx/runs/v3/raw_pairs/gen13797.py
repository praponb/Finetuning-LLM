from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .groupBy(
        F.quarter(F.col("ss.sale_date")).alias("sale_quarter"),
        F.year(F.col("ss.sale_date")).alias("sale_year"),
    )
    .agg(
        F.countDistinct(F.col("i.quantity")).alias("unique_stores"),
        F.round(F.avg(F.col("i.quantity")), 2).alias("avg_quantity"),
    )
    .orderBy("avg_quantity")
)
