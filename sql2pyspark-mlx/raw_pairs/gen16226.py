from pyspark.sql import functions as F

matched_keys = (
    df_accounts
    .select("status", "id")
    .exceptAll(
        df_accounts
        .filter(F.year("opened_date") >= 2025)
        .select("status", "id")
    )
)

kept_rows = (
    df_accounts
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy(F.date_trunc("WEEK", "opened_date").alias("week_start"))
    .agg(
        F.count(F.when(F.col("account_type") == "LOAN", 1)).alias("loan_count"),
        F.count(F.when(F.col("account_type") == "CREDIT", 1)).alias("credit_rows"),
        F.max(F.when(F.col("account_type") == "CREDIT", F.col("balance"))).alias("credit_max_balance"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.countDistinct("status").alias("distinct_status"),
    )
)
