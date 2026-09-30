from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .filter(F.month(F.col("s.opened_date")).isin([2, 5, 10]) | (F.col("s.city") == "Berlin"))
    .select(
        F.col("o.customer_id"),
        F.col("o.amount"),
        F.col("o.status"),
        F.col("s.store_name"),
        F.col("s.opened_date"),
        F.col("s.sqft"),
        F.concat_ws("-", F.col("s.store_name"), F.col("s.region")).alias("store_name_region_key"),
    )
    .orderBy("opened_date")
)
