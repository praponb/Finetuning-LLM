from pyspark.sql import functions as F

store_summary = (
    df_store_sales
    .groupBy("store_id")
    .agg(
        F.count("*").alias("cnt"),
        F.countDistinct("units").alias("unique_products"),
    )
)

result = (
    df_stores.alias("stores")
    .join(store_summary.alias("t"), F.col("stores.id") == F.col("t.store_id"), "left")
    .filter(F.col("t.unique_products") > 4)
    .select(
        F.col("stores.id"),
        F.col("stores.region"),
        F.col("t.cnt"),
        F.col("t.unique_products"),
    )
    .orderBy(F.col("unique_products").desc())
)
