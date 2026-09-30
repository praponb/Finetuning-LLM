from pyspark.sql import functions as F

result = (
    df_inventory
    .alias("a")
    .join(df_stores.alias("s"), F.col("a.store_id") == F.col("s.id"), "left")
    .groupBy(F.col("s.city").alias("city"))
    .agg(
        F.count("*").alias("rows_total"),
        F.sum(F.col("a.quantity")).alias("total_quantity"),
    )
)
