from pyspark.sql import functions as F

orders_by_customer = (
    df_orders
    .filter(F.col("customer_id").isNotNull())
    .groupBy("customer_id")
    .agg(
        F.countDistinct("discount").alias("unique_channels"),
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
)

result = (
    df_customers.alias("c")
    .join(orders_by_customer.alias("O"), F.col("c.id") == F.col("O.customer_id"), "inner")
    .select(
        F.col("c.segment"),
        F.col("c.country"),
        F.col("O.unique_channels"),
        F.col("O.status_values"),
    )
)
