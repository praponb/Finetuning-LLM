from pyspark.sql import functions as F

store_summary = (
    df_inventory
    .filter(F.col("quantity") < 176)
    .groupBy("store_id")
    .agg(F.max("quantity").alias("max_quantity"))
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left")
    .filter(F.col("ss.max_quantity") > 210)
    .select(
        F.col("s.id"),
        F.col("s.region"),
        F.col("s.opened_date"),
        F.col("ss.max_quantity"),
    )
    .orderBy(F.col("region").desc())
)
