from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.alias("o"), F.col("s.id") == F.col("o.store_id"), "left")
    .select(
        F.col("s.sqft"),
        F.col("s.region"),
        F.col("s.store_name"),
        F.col("o.shipping_fee"),
        F.col("o.amount"),
    )
    .orderBy("sqft")
)
