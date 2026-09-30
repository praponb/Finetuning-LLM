from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.col("o.order_date") >= "2023-06-01")
    .select(
        F.col("o.id"),
        F.col("o.customer_id"),
        F.col("o.status"),
        F.col("o.discount"),
        F.col("s.city"),
        F.col("s.sqft"),
        F.col("s.region"),
        F.concat(F.col("o.status"), F.lit(", "), F.col("o.channel")).alias("status_channel"),
    )
    .orderBy("status", F.col("city").desc(), "id")
    .limit(20)
)
