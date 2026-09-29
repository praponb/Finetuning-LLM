from pyspark.sql import functions as F

customer_summary = (
    df_orders
    .filter(F.col("discount").between(0.16, 0.32))
    .groupBy("customer_id")
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.sum("discount").alias("total_discount"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "left")
    .select(
        F.col("c.id"),
        F.col("c.country"),
        F.col("cs.status_list"),
        F.col("cs.total_discount"),
    )
    .orderBy("id", F.col("total_discount").desc())
)
