from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.alias("o"), F.col("o.store_id") == F.col("s.id"), "inner")
    .select(
        F.col("s.store_name"),
        F.col("s.opened_date"),
        F.col("o.amount"),
        F.col("o.status"),
        F.col("o.order_date"),
    )
    .orderBy(F.col("amount").desc())
)
