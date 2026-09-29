from pyspark.sql import functions as F

orders_by_customer = (
    df_orders
    .filter(F.col("status").isin(["RETURNED", "COMPLETED", "CANCELLED"]))
    .groupBy("customer_id")
    .agg(
        F.round(F.avg("shipping_fee"), 2).alias("avg_shipping_fee"),
        F.countDistinct("amount").alias("unique_stores"),
    )
)

result = (
    df_customers.alias("c")
    .join(orders_by_customer.alias("t"), F.col("c.id") == F.col("t.customer_id"), "left")
    .filter(F.col("t.avg_shipping_fee") > 61)
    .select(
        F.col("c.credit_limit"),
        F.col("c.signup_date"),
        F.col("t.avg_shipping_fee"),
        F.col("t.unique_stores"),
    )
)
