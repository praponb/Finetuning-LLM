from pyspark.sql import functions as F

store_summary = (
    df_orders
    .groupBy("store_id")
    .agg(
        F.sum(F.when(F.col("status") == "SHIPPED", F.col("discount")).otherwise(0)).alias("shipped_discount"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("S"), F.col("s.id") == F.col("S.store_id"), "left")
    .select(F.col("s.id"), F.col("s.store_name"), F.col("S.shipped_discount"))
    .orderBy(F.col("store_name").desc(), "id")
)
