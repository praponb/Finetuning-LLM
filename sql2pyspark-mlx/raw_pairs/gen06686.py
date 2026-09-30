from pyspark.sql import functions as F

store_summary = (
    df_store_sales
    .groupBy("store_id")
    .agg(F.countDistinct("revenue").alias("unique_stores"))
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left")
    .filter(F.col("ss.unique_stores") > 9)
    .select(
        F.col("s.id"),
        F.col("s.opened_date"),
        F.col("s.region"),
        F.col("ss.unique_stores"),
    )
    .orderBy("opened_date", F.col("unique_stores").desc())
)
