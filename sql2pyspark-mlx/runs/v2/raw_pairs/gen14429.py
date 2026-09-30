from pyspark.sql import functions as F

store_summary = (
    df_orders
    .groupBy("store_id")
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.sum("amount").alias("total_amount"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("S"), F.col("s.id") == F.col("S.store_id"), "left")
    .select(
        F.col("s.sqft"),
        F.col("s.city"),
        F.col("S.avg_amount"),
        F.col("S.total_amount"),
    )
    .orderBy(F.col("total_amount").desc())
)
