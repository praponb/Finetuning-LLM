from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.alias("o"), F.col("s.id") == F.col("o.store_id"), "inner")
    .filter(~F.col("o.channel").isin(["web", "store", "partner"]))
    .select(
        F.col("s.opened_date"),
        F.col("o.shipping_fee"),
        F.col("o.amount"),
        F.col("o.customer_id"),
        F.lower(F.col("s.city")).alias("city_lower"),
    )
)
