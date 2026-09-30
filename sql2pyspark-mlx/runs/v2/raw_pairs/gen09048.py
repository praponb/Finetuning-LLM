from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_orders.alias("orders"), F.col("order_items.order_id") == F.col("orders.id"), "left")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "left")
    .filter(F.col("customers.country") != "US")
    .select(
        F.col("order_items.id"),
        F.col("order_items.product_id"),
        F.col("order_items.unit_price"),
        F.col("orders.store_id"),
        F.col("orders.shipping_fee"),
        F.col("customers.credit_limit"),
        F.regexp_replace(F.col("customers.name"), " ", "-").alias("name_slug"),
    )
)
