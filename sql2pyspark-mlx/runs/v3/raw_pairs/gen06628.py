from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "left")
    .groupBy(F.quarter(F.col("s.opened_date")).alias("opened_quarter"))
    .agg(
        F.min(F.col("i.last_restock")).alias("min_last_restock"),
        F.count(F.col("i.quantity")).alias("quantity_count"),
    )
    .orderBy("quantity_count", F.col("opened_quarter").desc())
)
