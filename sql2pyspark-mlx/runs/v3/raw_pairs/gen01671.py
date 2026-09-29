from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "outer")
    .filter(F.col("s.sqft") < 15097)
    .select(
        F.col("o.amount"),
        F.col("s.region"),
        F.col("s.store_name"),
        F.col("s.opened_date"),
    )
)
