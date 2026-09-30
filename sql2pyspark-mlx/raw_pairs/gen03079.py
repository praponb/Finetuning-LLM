from pyspark.sql import functions as F

customer_stats = (
    df_accounts
    .filter(F.col("opened_date").between("2025-01-01", "2025-06-01"))
    .groupBy("customer_id")
    .agg(
        F.sum(F.when(F.col("status") == "FROZEN", 1).otherwise(0)).alias("frozen_count"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("C"), F.col("c.id") == F.col("C.customer_id"), "inner")
    .filter(F.col("C.frozen_count") >= 26)
    .select(
        F.col("c.id"),
        F.col("c.age"),
        F.col("c.credit_limit"),
        F.col("C.frozen_count"),
    )
    .orderBy(F.col("frozen_count").desc())
)
