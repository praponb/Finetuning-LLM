from pyspark.sql import functions as F

customer_summary = (
    df_orders
    .filter(~F.col("status").isin(["COMPLETED", "PENDING"]))
    .groupBy("customer_id")
    .agg(F.max("order_date").alias("latest_order_date"))
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "left")
    .select(
        F.col("c.id"),
        F.col("c.credit_limit"),
        F.col("c.country"),
        F.col("cs.latest_order_date"),
    )
)
