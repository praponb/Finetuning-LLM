from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.alias("o"), F.col("o.store_id") == F.col("s.id"), "left")
    .groupBy(F.col("s.id"), F.col("s.store_name"))
    .agg(
        F.count(F.col("o.id")).alias("order_count"),
        F.sum(F.col("o.amount")).alias("total_amount"),
        (F.max(F.col("o.discount")) - F.min(F.col("o.discount"))).alias("discount_range"),
    )
    .orderBy("id", "store_name")
    .limit(10)
)
