from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "right")
    .filter(F.col("i.last_restock").between("2024-03-05", "2025-09-01"))
    .groupBy(F.col("s.region"))
    .agg(
        F.max(F.col("i.last_restock")).alias("latest_last_restock"),
        F.avg(F.col("i.quantity")).alias("avg_quantity"),
    )
    .filter(F.col("avg_quantity") > 140)
    .orderBy("region")
)
