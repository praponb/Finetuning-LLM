from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .filter(F.col("s.city").isin(["Berlin", "Singapore", "Bangkok"]))
    .groupBy(F.date_format(F.col("s.opened_date"), "yyyy-MM").alias("opened_month"))
    .agg(
        F.count(F.col("i.quantity")).alias("quantity_count"),
        F.max(F.col("i.last_restock")).alias("latest_last_restock"),
    )
)
