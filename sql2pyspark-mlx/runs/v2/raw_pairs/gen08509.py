from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "right")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "inner")
    .filter(~F.col("customers.country").isin(["US", "TH"]))
    .groupBy(F.col("customers.is_active"), F.col("customers.country"))
    .agg(
        F.min(F.col("orders.order_date")).alias("first_order_date"),
        F.count("*").alias("num_orders"),
    )
    .orderBy(F.col("num_orders").desc(), "is_active", "country")
    .limit(10)
)
