from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .select(
        F.col("o.discount"),
        F.col("c.is_active"),
        F.col("c.city"),
        F.col("s.opened_date"),
        F.col("s.store_name"),
        F.coalesce(F.col("c.segment"), F.lit("unassigned")).alias("segment_filled"),
    )
    .orderBy("store_name", "segment_filled")
)
