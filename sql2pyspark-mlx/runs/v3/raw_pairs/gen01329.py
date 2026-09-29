from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.col("o.amount").isNotNull() & (F.year(F.col("s.opened_date")) == 2025))
    .select(F.col("o.id"), F.col("o.shipping_fee"), F.col("s.sqft"))
)
