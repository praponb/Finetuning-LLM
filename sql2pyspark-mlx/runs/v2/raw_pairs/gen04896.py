from pyspark.sql import functions as F

store_sales_by_store = (
    df_store_sales
    .groupBy("store_id")
    .agg(
        F.round(F.stddev("units"), 2).alias("stddev_units"),
        F.count("revenue").alias("revenue_count"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_sales_by_store.alias("ssbs"), F.col("s.id") == F.col("ssbs.store_id"), "left")
    .select(
        F.col("s.store_name"),
        F.col("s.opened_date"),
        F.col("ssbs.stddev_units"),
        F.col("ssbs.revenue_count"),
    )
)
