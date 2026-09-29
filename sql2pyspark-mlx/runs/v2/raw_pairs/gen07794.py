from pyspark.sql import functions as F

store_sales_by_store = (
    df_store_sales
    .groupBy("store_id")
    .agg(
        F.max("sale_date").alias("latest_sale_date"),
        F.avg("units").alias("avg_units"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_sales_by_store.alias("ssbs"), F.col("s.id") == F.col("ssbs.store_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.region"),
        F.col("ssbs.latest_sale_date"),
        F.col("ssbs.avg_units"),
    )
    .orderBy("avg_units")
)
