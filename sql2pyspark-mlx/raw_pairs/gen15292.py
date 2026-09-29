from pyspark.sql import functions as F

matched_keys = (
    df_accounts
    .select("account_type", "id")
    .subtract(
        df_accounts
        .filter(F.col("opened_date") < "2024-09-01")
        .select("account_type", "id")
    )
)

verified = (
    df_accounts
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.account_type") == F.col("k.account_type")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(F.date_trunc("WEEK", "opened_date").alias("week_start"))
    .agg(
        F.round(F.sum("balance") / F.nullif(F.count(F.when(F.col("status") == "CLOSED", 1)), F.lit(0)), 2).alias("balance_per_closed"),
        F.round(F.sum("balance") / F.nullif(F.count(F.when(F.col("status") == "CLOSED", 1)), F.lit(0)), 2).alias("balance_per_closed_2"),
        F.round(F.sum("balance") / F.nullif(F.count(F.when(F.col("status") == "OPEN", 1)), F.lit(0)), 2).alias("balance_per_open"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.count(F.when(F.col("account_type") == "SAVINGS", 1)).alias("savings_rows"),
    )
    .orderBy("balance_per_closed_2")
)
