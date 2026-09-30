from pyspark.sql import functions as F

store_sales_by_store = (
    df_store_sales
    .groupBy("store_id")
    .agg(
        F.countDistinct("revenue").alias("unique_stores"),
        F.max("units").alias("max_units"),
    )
)

result = (
    df_stores.alias("stores")
    .join(store_sales_by_store.alias("t"), F.col("stores.id") == F.col("t.store_id"), "inner")
    .filter(F.col("t.max_units") > 24)
    .select(
        F.col("stores.opened_date"),
        F.col("stores.region"),
        F.col("t.unique_stores"),
        F.col("t.max_units"),
    )
    .orderBy(F.col("region").desc())
)
