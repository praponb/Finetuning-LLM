from pyspark.sql import functions as F

cleaned = (
    df_accounts
    .filter((F.col("opened_date") >= F.lit("2023-03-04").cast("date")) & F.col("customer_id").isNotNull())
    .select("*", F.round("balance", 0).alias("balance_rounded"))
)

result = (
    cleaned
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "inner")
    .groupBy(F.col("p.country").alias("country"), F.col("s.status").alias("status"))
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.sum(F.col("s.balance")), 2).alias("total_balance"),
    )
    .orderBy("country")
)
