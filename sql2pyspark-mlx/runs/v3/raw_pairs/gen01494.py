from pyspark.sql import functions as F

result = (
    df_stores.alias("stores")
    .join(df_orders.filter(F.col("store_id").isNull()).alias("orders"), F.col("stores.id") == F.col("orders.store_id"), "left_anti")
    .filter(F.col("stores.city") == "London")
    .select(F.col("stores.id"), F.col("stores.opened_date"))
    .orderBy(F.col("id").desc())
)
