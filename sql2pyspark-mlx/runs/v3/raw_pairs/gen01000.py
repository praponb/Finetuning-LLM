from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_inventory.alias("i"), F.col("s.id") == F.col("i.store_id"), "left")
    .filter(F.month(F.col("s.opened_date")).isin([7, 9, 10]) | F.upper(F.col("s.store_name")).like("%LD %"))
    .select(
        F.col("s.city"),
        F.col("s.sqft"),
        F.col("s.opened_date"),
        F.col("i.last_restock"),
        F.col("i.store_id"),
    )
)
