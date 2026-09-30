from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.alias("o"), F.col("s.id") == F.col("o.store_id"), "left")
    .filter(F.col("s.store_name").like("A%"))
    .groupBy(F.col("s.id"))
    .agg(
        F.count(F.col("o.id")).alias("order_count"),
        F.sum(F.col("o.discount")).alias("total_discount"),
        F.sum(F.when(F.col("o.status") == "COMPLETED", 1).otherwise(0)).alias("completed_count"),
    )
    .filter(F.col("total_discount") >= 2.0)
    .orderBy("order_count")
)
