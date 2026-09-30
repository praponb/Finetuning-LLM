from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_stores.alias("stores"), F.col("inventory.store_id") == F.col("stores.id"), "inner")
    .groupBy(F.quarter(F.col("stores.opened_date")).alias("opened_quarter"))
    .agg(F.avg(F.col("inventory.quantity")).alias("avg_quantity"))
    .orderBy(F.col("avg_quantity").desc())
)
