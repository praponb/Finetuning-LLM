from pyspark.sql import functions as F

store_sales_by_store = (
    df_store_sales
    .groupBy("store_id")
    .agg(F.max("sale_date").alias("last_sale_date"))
)

result = (
    df_stores.alias("s")
    .join(store_sales_by_store.alias("ssbs"), F.col("s.id") == F.col("ssbs.store_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.city"),
        F.col("s.store_name"),
        F.col("ssbs.last_sale_date"),
    )
    .orderBy("last_sale_date", "store_name")
)
