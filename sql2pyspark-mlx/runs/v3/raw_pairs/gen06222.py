from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "inner")
    .filter(F.col("orders.customer_id").isNotNull() & F.col("stores.region").isin(["West", "Central"]))
    .select(
        F.col("orders.id"),
        F.col("orders.status"),
        F.col("stores.region"),
        F.col("stores.opened_date"),
        F.col("stores.sqft"),
    )
    .orderBy(F.col("sqft").desc(), F.col("status").desc())
)
