from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.filter(F.col("shipping_fee").between(11, 39)).alias("o"), F.col("s.id") == F.col("o.store_id"), "left_semi")
    .select(F.col("s.id"), F.col("s.city"), F.col("s.store_name"), F.col("s.sqft"))
)
