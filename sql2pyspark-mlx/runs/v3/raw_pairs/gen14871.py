from pyspark.sql import functions as F

store_sales_by_store = (
    df_store_sales
    .filter(F.col("units").isNull())
    .groupBy("store_id")
    .agg(
        F.round(F.stddev("units"), 2).alias("stddev_units"),
        F.countDistinct("revenue").alias("unique_products"),
    )
)

result = (
    df_stores.alias("stores")
    .join(store_sales_by_store.alias("t"), F.col("stores.id") == F.col("t.store_id"), "inner")
    .filter(F.col("t.stddev_units") > 58)
    .select(
        F.col("stores.id"),
        F.col("stores.opened_date"),
        F.col("t.stddev_units"),
        F.col("t.unique_products"),
    )
    .orderBy(F.col("id").desc())
    .limit(5)
)
