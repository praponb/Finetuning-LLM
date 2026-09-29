from pyspark.sql import functions as F

rollup_totals = (
    df_accounts
    .alias("s")
    .join(df_customers.alias("dim"), F.col("s.customer_id") == F.col("dim.id"), "inner")
    .groupingSets(
        [["country", "city"], ["country"], []],
        F.col("dim.country").alias("country"),
        F.col("dim.city").alias("city"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.col("s.balance")).alias("max_balance"),
    )
)

result = (
    rollup_totals
    .select("num_rows", "max_balance")
    .orderBy(F.col("num_rows").asc_nulls_first())
)
