from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "right")
    .filter(F.col("c.is_active"))
    .groupBy(F.col("c.city"))
    .agg(
        F.count(F.when(F.col("a.status") == "OPEN", 1)).alias("open_count"),
        F.count("*").alias("num_accounts"),
    )
)
