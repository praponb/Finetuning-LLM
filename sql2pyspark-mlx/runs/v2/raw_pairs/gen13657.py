from pyspark.sql import functions as F

result = (
    df_stores.alias("stores")
    .join(df_orders.filter(F.col("customer_id") == 23).alias("orders"), F.col("stores.id") == F.col("orders.store_id"), "left_anti")
    .filter(~F.col("stores.store_name").like("D%"))
    .select(F.col("stores.id"), F.col("stores.city"))
    .orderBy("id")
)
