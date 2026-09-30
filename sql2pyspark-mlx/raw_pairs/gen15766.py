from pyspark.sql import functions as F

excluded_stores = df_stores.filter(F.col("sqft") > 13134).select("id")

result = (
    df_orders.alias("orders")
    .join(excluded_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "left_anti")
    .filter(F.col("orders.store_id").isNotNull() | F.lit(excluded_stores.isEmpty()))
    .select(F.col("orders.id"), F.col("orders.customer_id"), F.col("orders.discount"))
)
