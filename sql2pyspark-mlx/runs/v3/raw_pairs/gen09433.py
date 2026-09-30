from pyspark.sql import functions as F

store_sales_by_store = (
    df_store_sales
    .groupBy("store_id")
    .agg(F.max("revenue").alias("max_revenue"))
)

result = (
    df_stores.alias("s")
    .join(store_sales_by_store.alias("ssbs"), F.col("s.id") == F.col("ssbs.store_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.sqft"),
        F.col("s.store_name"),
        F.col("ssbs.max_revenue"),
    )
    .orderBy(F.col("sqft").desc())
)
