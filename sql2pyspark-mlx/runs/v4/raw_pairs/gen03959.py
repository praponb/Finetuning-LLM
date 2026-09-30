from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "left")
    .filter(F.col("s.sqft").between(8528, 15220))
    .groupBy(F.quarter(F.col("s.opened_date")).alias("opened_quarter"))
    .agg(
        F.sum(F.col("i.quantity")).alias("total_quantity"),
        F.max(F.col("i.last_restock")).alias("latest_last_restock"),
    )
    .orderBy("total_quantity")
)
