from pyspark.sql import functions as F

orders_by_customer = (
    df_orders
    .filter(F.col("order_date") <= "2024-01-09")
    .groupBy("customer_id")
    .agg(
        F.max("amount").alias("max_amount"),
        F.round(((100 * F.sum(F.when(F.col("status") == "SHIPPED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_shipped"),
    )
)

result = (
    df_customers.alias("customers")
    .join(orders_by_customer.alias("orders_by_customer"), F.col("customers.id") == F.col("orders_by_customer.customer_id"), "left")
    .filter(F.col("orders_by_customer.pct_shipped") > 31)
    .select(
        F.col("customers.id"),
        F.col("customers.segment"),
        F.col("customers.name"),
        F.col("orders_by_customer.max_amount"),
        F.col("orders_by_customer.pct_shipped"),
    )
    .orderBy(F.col("id").desc(), F.col("pct_shipped").desc())
)
