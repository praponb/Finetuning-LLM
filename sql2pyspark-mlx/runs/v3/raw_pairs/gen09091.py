from pyspark.sql import functions as F

customer_totals = (
    df_sessions
    .groupBy("customer_id")
    .agg(
        F.count("*").alias("total_count"),
        F.count(F.when(F.col("browser") == "Chrome", 1)).alias("chrome_count"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("ct"), F.col("c.id") == F.col("ct.customer_id"), "inner")
    .select(
        F.col("c.country"),
        F.col("c.credit_limit"),
        F.col("ct.total_count"),
        F.col("ct.chrome_count"),
    )
    .orderBy("country")
)
