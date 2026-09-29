from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(F.date_format("opened_date", "yyyy-MM").alias("opened_month"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.count(F.when(F.col("account_type") == "SAVINGS", 1)).alias("savings_count"),
        F.max("balance").alias("max_balance"),
    )
    .orderBy("opened_month", F.col("savings_count").desc())
)
