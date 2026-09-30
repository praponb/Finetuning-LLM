from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .filter((F.col("s.city") == "Berlin") | F.col("o.amount").between(770, 1550))
    .select(
        F.col("o.id"),
        F.col("o.status"),
        F.col("o.customer_id"),
        F.col("s.opened_date"),
        F.col("s.region"),
    )
)
