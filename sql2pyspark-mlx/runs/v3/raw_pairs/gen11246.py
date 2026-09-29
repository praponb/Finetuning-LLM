from pyspark.sql import functions as F

store_sales_by_store = (
    df_store_sales
    .groupBy("store_id")
    .agg(F.round(F.stddev("units"), 2).alias("stddev_units"))
)

result = (
    df_stores.alias("stores")
    .join(store_sales_by_store.alias("store_sales_by_store"), F.col("stores.id") == F.col("store_sales_by_store.store_id"), "inner")
    .filter(F.col("store_sales_by_store.stddev_units") > 34)
    .select(
        F.col("stores.sqft"),
        F.col("stores.opened_date"),
        F.col("store_sales_by_store.stddev_units"),
    )
    .orderBy("stddev_units", F.col("sqft").desc())
)
