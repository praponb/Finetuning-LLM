from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.alias("o"), F.col("o.store_id") == F.col("s.id"), "left")
    .filter(F.col("o.status") == "SHIPPED")
    .select(
        F.col("s.sqft"),
        F.col("s.region"),
        F.col("o.customer_id"),
        F.col("o.channel"),
    )
)
