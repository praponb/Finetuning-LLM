from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.alias("o"), F.col("s.id") == F.col("o.store_id"), "left")
    .filter(~F.col("o.channel").isin(["partner", "web", "store"]))
    .select(
        F.col("s.city"),
        F.col("s.sqft"),
        F.col("s.region"),
        F.col("o.customer_id"),
        F.col("o.status"),
    )
    .orderBy("region")
)
