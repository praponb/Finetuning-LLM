from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.filter(F.col("customer_id").isin([8, 10, 25, 37])).alias("o"), F.col("s.id") == F.col("o.store_id"), "left_anti")
    .select(F.col("s.id"), F.col("s.store_name"), F.col("s.sqft"))
)
