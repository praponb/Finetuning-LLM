from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.filter(F.col("merchant").isin(["Starbucks", "Grab", "7-Eleven"])).alias("t"), F.col("a.id") == F.col("t.account_id"), "left_semi")
    .filter(F.col("a.opened_date").between("2025-02-17", "2025-09-01"))
    .select(
        F.col("a.id"),
        F.col("a.status"),
        F.col("a.account_type"),
        F.col("a.opened_date"),
    )
    .orderBy(F.col("opened_date").desc())
)
