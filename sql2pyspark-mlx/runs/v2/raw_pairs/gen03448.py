from pyspark.sql import functions as F

store_summary = (
    df_store_sales
    .filter(F.col("store_id") == 3)
    .groupBy("store_id")
    .agg(
        F.countDistinct("revenue").alias("unique_stores"),
        F.max("units").alias("max_units"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left")
    .filter(F.col("ss.unique_stores") >= 4)
    .select(
        F.col("s.id"),
        F.col("s.city"),
        F.col("ss.unique_stores"),
        F.col("ss.max_units"),
    )
)
