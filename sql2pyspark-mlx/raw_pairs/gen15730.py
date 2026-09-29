from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(F.date_trunc("week", "opened_date").alias("week_start"))
    .agg(
        F.count(F.when(F.col("account_type") == "SAVINGS", 1)).alias("savings_count"),
        F.round(F.sum("balance") / F.nullif(F.count(F.when(F.col("status") == "OPEN", 1)), F.lit(0)), 2).alias("balance_per_open"),
        F.count("*").alias("cnt"),
    )
    .orderBy("cnt")
)
