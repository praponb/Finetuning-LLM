from pyspark.sql import functions as F

customer_totals = (
    df_orders
    .groupBy("customer_id")
    .agg(
        F.round(F.stddev("shipping_fee"), 2).alias("stddev_shipping_fee"),
        F.count("amount").alias("amount_count"),
    )
)

result = (
    df_customers.alias("customers")
    .join(customer_totals.alias("customer_totals"), F.col("customers.id") == F.col("customer_totals.customer_id"), "inner")
    .select(
        F.col("customers.credit_limit"),
        F.col("customers.email"),
        F.col("customer_totals.stddev_shipping_fee"),
        F.col("customer_totals.amount_count"),
    )
    .orderBy(F.col("stddev_shipping_fee").desc())
)
