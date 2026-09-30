from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "inner")
    .filter(F.year(F.col("s.opened_date")) >= 2023)
    .groupBy(F.col("s.city"))
    .agg(
        F.min(F.col("i.last_restock")).alias("min_last_restock"),
        F.min(F.col("i.last_restock")).alias("first_last_restock"),
    )
)
