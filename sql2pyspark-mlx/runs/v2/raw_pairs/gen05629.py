from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter((F.col("s.region") != "West") & (F.col("o.channel") != "partner"))
    .select(
        F.col("o.id"),
        F.col("o.customer_id"),
        F.col("o.status"),
        F.col("s.opened_date"),
        F.col("s.store_name"),
        F.col("s.sqft"),
    )
    .orderBy("store_name", "id")
    .limit(1)
)
