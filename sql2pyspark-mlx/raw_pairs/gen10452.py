from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.alias("o"), F.col("s.id") == F.col("o.store_id"), "left")
    .select(
        F.col("s.region"),
        F.col("s.store_name"),
        F.col("s.opened_date"),
        F.col("o.store_id"),
    )
    .orderBy("store_name")
)
