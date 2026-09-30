from pyspark.sql import functions as F

customer_summary = (
    df_accounts
    .groupBy("customer_id")
    .agg(
        F.min("opened_date").alias("min_opened_date"),
        F.max("balance").alias("max_balance"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("t"), F.col("c.id") == F.col("t.customer_id"), "left")
    .filter(F.col("t.max_balance") >= 44000)
    .select(
        F.col("c.id"),
        F.col("c.name"),
        F.col("c.country"),
        F.col("t.min_opened_date"),
        F.col("t.max_balance"),
    )
    .orderBy(F.col("id").desc())
)
