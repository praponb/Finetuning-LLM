from pyspark.sql import functions as F

customer_totals = (
    df_sessions
    .filter(F.col("customer_id").isin([5, 10, 23]))
    .groupBy("customer_id")
    .agg(
        F.min("started_at").alias("min_started_at"),
        F.sum(F.when(F.col("browser") == "Edge", 1).otherwise(0)).alias("edge_count"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("ct"), F.col("c.id") == F.col("ct.customer_id"), "inner")
    .filter(F.col("ct.edge_count") >= 52)
    .select(
        F.col("c.id"),
        F.col("c.credit_limit"),
        F.col("ct.min_started_at"),
        F.col("ct.edge_count"),
    )
)
