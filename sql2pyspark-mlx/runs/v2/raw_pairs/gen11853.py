from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "left")
    .filter(F.col("s.sqft").between(8812, 12221))
    .select(
        F.col("i.id"),
        F.col("i.store_id"),
        F.col("i.product_id"),
        F.col("s.opened_date"),
        F.date_sub(F.col("i.last_restock"), 84).alias("last_restock_minus_84d"),
    )
    .orderBy(F.col("opened_date").desc(), "id")
    .limit(5)
)
