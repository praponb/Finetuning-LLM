from pyspark.sql import functions as F

customer_summary = (
    df_orders
    .filter(F.col("order_date") < "2023-03-01")
    .groupBy("customer_id")
    .agg(F.sum("shipping_fee").alias("total_shipping_fee"))
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "left")
    .filter(F.col("cs.total_shipping_fee") > 200)
    .select(
        F.col("c.id"),
        F.col("c.name"),
        F.col("c.credit_limit"),
        F.col("cs.total_shipping_fee"),
    )
)
