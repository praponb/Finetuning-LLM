from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.alias("o"), F.col("s.id") == F.col("o.store_id"), "left")
    .filter(F.col("s.sqft").isNotNull())
    .select(
        F.col("s.opened_date"),
        F.col("s.city"),
        F.col("s.sqft"),
        F.col("o.customer_id"),
        F.col("o.discount"),
        F.col("o.store_id"),
    )
)
