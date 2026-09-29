from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "left")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "inner")
    .filter(F.col("stores.sqft") < 15875)
    .select(
        F.col("orders.discount"),
        F.col("orders.status"),
        F.col("customers.name"),
        F.col("customers.credit_limit"),
        F.col("stores.sqft"),
        F.col("stores.store_name"),
        (F.col("orders.shipping_fee") * F.col("orders.amount")).alias("amount_with_shipping"),
    )
)
