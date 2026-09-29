from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_inventory.alias("i"), F.col("s.id") == F.col("i.store_id"), "left_anti")
    .filter(F.col("s.sqft") == 5577)
    .select(
        F.col("s.id"),
        F.col("s.region"),
        F.col("s.store_name"),
        F.col("s.opened_date"),
    )
)
