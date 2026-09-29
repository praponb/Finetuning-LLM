from pyspark.sql import functions as F

customer_stats = (
    df_accounts
    .filter(F.col("balance") >= 24000)
    .groupBy("customer_id")
    .agg(
        F.countDistinct("balance").alias("unique_statuss"),
        F.sum("balance").alias("total_balance"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "inner")
    .filter(F.col("cs.unique_statuss") >= 10)
    .select(
        F.col("c.id"),
        F.col("c.name"),
        F.col("c.country"),
        F.col("cs.unique_statuss"),
        F.col("cs.total_balance"),
    )
    .orderBy("unique_statuss", F.col("id").desc())
)
