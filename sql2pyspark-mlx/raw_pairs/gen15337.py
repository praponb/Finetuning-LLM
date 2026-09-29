from pyspark.sql import functions as F

store_summary = (
    df_inventory
    .groupBy("store_id")
    .agg(
        F.count("*").alias("num_inventory"),
        F.sum("quantity").alias("total_quantity"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left")
    .select(
        F.col("s.opened_date"),
        F.col("ss.num_inventory"),
        F.col("ss.total_quantity"),
    )
    .orderBy("opened_date")
)
