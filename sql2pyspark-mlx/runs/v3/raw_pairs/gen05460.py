from pyspark.sql import functions as F

src = (
    df_accounts
    .filter((F.col("balance") >= 51000) & F.col("account_type").isin(["LOAN", "CREDIT", "SAVINGS"]) & F.col("opened_date").between("2025-07-01", "2025-10-01"))
)

accounts_stats = (
    src
    .groupBy(
        F.when(F.col("status").isin(["FROZEN", "CLOSED"]), "group_a").otherwise("other").alias("status_group"),
    )
    .agg(
        F.count("balance").alias("balance_count"),
        F.max("opened_date").alias("last_opened_date"),
    )
)

result = (
    accounts_stats
    .filter(F.col("balance_count") < 8)
    .select("status_group", "balance_count")
    .orderBy(F.col("balance_count").desc(), "status_group")
    .limit(5)
)
