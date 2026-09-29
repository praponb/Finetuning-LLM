from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "left")
    .filter(F.col("s.sqft").between(11287, 12884))
    .select(
        F.col("i.store_id"),
        F.col("s.opened_date"),
        F.col("s.region"),
        F.concat_ws(",", F.col("s.store_name"), F.col("s.city")).alias("store_name_city_key"),
    )
)
