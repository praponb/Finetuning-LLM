from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .groupBy(F.quarter(F.col("ss.sale_date")).alias("sale_quarter"))
    .agg(F.max(F.col("i.quantity")).alias("max_quantity"))
    .filter(F.col("max_quantity") > 60)
)
