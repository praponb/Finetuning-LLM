from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(~F.col("s.city").isin(["London", "Chiang Mai"]))
    .select(
        F.col("o.id"),
        F.col("o.order_date"),
        F.col("s.sqft"),
        F.col("s.city"),
        F.col("s.opened_date"),
    )
    .orderBy("opened_date")
)
