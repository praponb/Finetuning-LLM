from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter((F.col("s.region") == "Central") & F.col("o.amount").isNull())
    .select(F.col("o.shipping_fee"), F.col("s.region"), F.col("s.sqft"))
    .orderBy(F.col("shipping_fee").desc())
)
