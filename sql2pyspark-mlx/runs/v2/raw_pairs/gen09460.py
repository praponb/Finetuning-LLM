from pyspark.sql import functions as F

customer_summary = (
    df_orders
    .groupBy("customer_id")
    .agg(
        F.countDistinct("amount").alias("unique_statuss"),
        F.count("*").alias("order_count"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("t"), F.col("c.id") == F.col("t.customer_id"), "inner")
    .filter(F.col("t.unique_statuss") > 5)
    .select(
        F.col("c.id"),
        F.col("c.credit_limit"),
        F.col("t.unique_statuss"),
        F.col("t.order_count"),
    )
    .orderBy("unique_statuss")
)
