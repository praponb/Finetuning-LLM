from pyspark.sql import functions as F

valid = (
    df_accounts
    .filter(F.col("status").isNotNull() & (~F.col("status").eqNullSafe(F.lit("CLOSED"))))
    .select(
        "*",
        F.add_months("opened_date", 1).alias("opened_date_plus_months"),
        F.coalesce("status", F.lit("UNKNOWN")).alias("resolved_status"),
    )
)

result = (
    valid
    .groupBy(
        F.date_trunc("WEEK", "opened_date_plus_months").alias("week_start"),
        F.date_trunc("QUARTER", "opened_date").alias("quarter_start"),
    )
    .agg(
        F.sort_array(F.collect_set("account_type")).alias("account_type_list"),
        F.stddev_samp("balance").alias("balance_stddev"),
        F.count_if(F.col("account_type") == "CREDIT").alias("credit_events"),
        F.count(F.when(F.col("account_type") == "SAVINGS", 1)).alias("savings_count"),
    )
)
