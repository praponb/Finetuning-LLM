from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.filter(F.col("shipping_fee") > 14).alias("o"), F.col("s.id") == F.col("o.store_id"), "left_semi")
    .filter(F.col("s.city").isin(["Tokyo", "New York", "London"]))
    .select(F.col("s.id"), F.col("s.opened_date"), F.col("s.sqft"), F.col("s.region"))
)
