from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.filter(F.col("discount") > 0.19).alias("o"), F.col("s.id") == F.col("o.store_id"), "left_anti")
    .select(
        F.col("s.id"),
        F.col("s.opened_date"),
        F.col("s.store_name"),
        F.col("s.city"),
    )
)
