from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "right")
    .filter(F.col("o.status").isin(["COMPLETED", "RETURNED"]))
    .select(
        F.col("o.store_id"),
        F.col("o.amount"),
        F.col("c.country"),
        F.col("s.sqft"),
        F.lower(F.col("s.store_name")).alias("store_name_lower"),
    )
)
