from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "inner")
    .filter(F.col("i.store_id").isin([2, 4]))
    .groupBy(F.quarter(F.col("s.opened_date")).alias("opened_quarter"))
    .agg(
        F.max(F.col("i.quantity")).alias("max_quantity"),
        F.sum(F.col("i.quantity")).alias("total_quantity"),
    )
)
