from pyspark.sql import functions as F

store_stats = (
    df_orders
    .groupBy("store_id")
    .agg(
        F.sum(F.when(F.col("channel") == "partner", 1).otherwise(0)).alias("partner_count"),
    )
)

result = (
    df_stores.alias("stores")
    .join(store_stats.alias("t"), F.col("stores.id") == F.col("t.store_id"), "left")
    .select(
        F.col("stores.id"),
        F.col("stores.store_name"),
        F.col("stores.opened_date"),
        F.col("t.partner_count"),
    )
    .orderBy(F.col("id").desc(), "opened_date")
)
