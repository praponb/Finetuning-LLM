from pyspark.sql import functions as F

store_sales_by_store = (
    df_store_sales
    .filter(F.year("sale_date") == 2025)
    .groupBy("store_id")
    .agg(F.round(F.stddev("revenue"), 2).alias("stddev_revenue"))
)

result = (
    df_stores.alias("s")
    .join(store_sales_by_store.alias("ssbs"), F.col("s.id") == F.col("ssbs.store_id"), "inner")
    .filter(F.col("ssbs.stddev_revenue") > 73)
    .select(F.col("s.id"), F.col("s.city"), F.col("ssbs.stddev_revenue"))
)
