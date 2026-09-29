from pyspark.sql import functions as F

customer_stats = (
    df_accounts
    .groupBy("customer_id")
    .agg(
        F.min("opened_date").alias("min_opened_date"),
        F.count("*").alias("row_count"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "inner")
    .select(
        F.col("c.city"),
        F.col("c.name"),
        F.col("cs.min_opened_date"),
        F.col("cs.row_count"),
    )
    .orderBy(F.col("city").desc())
)
