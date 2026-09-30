from pyspark.sql import functions as F

base = (
    df_accounts
    .filter(~F.col("status").isin(["OPEN", "FROZEN", "CLOSED"]) & (F.col("balance") >= 72000))
)

accounts_stats = (
    base
    .groupBy(
        "status",
        F.when(F.year("opened_date") < 2024, "before_2024").when(F.year("opened_date") == 2024, "2024").otherwise("after_2024").alias("opened_cohort"),
    )
    .agg(
        F.round(F.avg("balance"), 2).alias("avg_balance"),
        F.concat_ws(", ", F.sort_array(F.collect_set("account_type"))).alias("account_type_values"),
    )
)

result = (
    accounts_stats
    .select("status", "opened_cohort", "avg_balance", "account_type_values")
)
