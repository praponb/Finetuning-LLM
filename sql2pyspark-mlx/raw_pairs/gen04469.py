from pyspark.sql import functions as F

customer_stats = (
    df_accounts
    .groupBy("customer_id")
    .agg(
        F.sum(F.when(F.col("status") == "FROZEN", 1).otherwise(0)).alias("frozen_count"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("t"), F.col("c.id") == F.col("t.customer_id"), "left")
    .filter(F.col("t.frozen_count") >= 75)
    .select(
        F.col("c.id"),
        F.col("c.city"),
        F.col("c.signup_date"),
        F.col("t.frozen_count"),
    )
    .orderBy(F.col("id").desc())
)
