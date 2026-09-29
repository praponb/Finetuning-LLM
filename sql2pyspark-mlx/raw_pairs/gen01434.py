from pyspark.sql import functions as F

result = (
    df_stores.alias("stores")
    .join(df_orders.alias("orders"), F.col("stores.id") == F.col("orders.store_id"), "left_anti")
    .select(F.col("stores.id"), F.col("stores.opened_date"), F.col("stores.sqft"))
)
