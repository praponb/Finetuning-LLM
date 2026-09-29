from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "left")
    .filter(F.col("o.customer_id").isin([18, 24, 34, 38]))
    .select(
        F.col("o.id"),
        F.col("o.channel"),
        F.col("o.discount"),
        F.col("s.city"),
        F.col("s.opened_date"),
        F.col("s.region"),
    )
)
