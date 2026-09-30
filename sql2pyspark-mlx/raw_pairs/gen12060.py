from pyspark.sql import functions as F

store_sales_by_store = (
    df_store_sales
    .filter(F.col("store_id").isin([2, 3, 6]))
    .groupBy("store_id")
    .agg(F.max("units").alias("max_units"))
)

result = (
    df_stores.alias("s")
    .join(store_sales_by_store.alias("ssbs"), F.col("s.id") == F.col("ssbs.store_id"), "left")
    .filter(F.col("ssbs.max_units") > 17)
    .select(
        F.col("s.id"),
        F.col("s.store_name"),
        F.col("s.opened_date"),
        F.col("ssbs.max_units"),
    )
    .orderBy("id", "store_name")
)
