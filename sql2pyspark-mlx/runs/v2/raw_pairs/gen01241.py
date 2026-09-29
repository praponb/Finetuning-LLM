from pyspark.sql import functions as F

orders_by_customer = (
    df_orders
    .filter(F.col("store_id").isNull())
    .groupBy("customer_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
)

result = (
    df_customers.alias("customers")
    .join(orders_by_customer.alias("OrdersByCustomer"), F.col("customers.id") == F.col("OrdersByCustomer.customer_id"), "left")
    .select(
        F.col("customers.id"),
        F.col("customers.age"),
        F.col("OrdersByCustomer.status_values"),
    )
    .orderBy("status_values")
)
