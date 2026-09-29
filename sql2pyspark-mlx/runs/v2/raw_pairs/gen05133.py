from pyspark.sql import functions as F

store_totals = (
    df_orders
    .filter(F.col("customer_id") == 1)
    .groupBy("store_id")
    .agg(F.round(F.avg("discount"), 2).alias("avg_discount"))
)

result = (
    df_stores.alias("s")
    .join(store_totals.alias("t"), F.col("s.id") == F.col("t.store_id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.store_name"),
        F.col("s.opened_date"),
        F.col("t.avg_discount"),
    )
    .orderBy(F.col("opened_date").desc(), "store_name")
)
