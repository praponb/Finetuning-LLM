from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.filter(F.col("merchant") == "7-Eleven").alias("t"), F.col("a.id") == F.col("t.account_id"), "left_semi")
    .filter(F.col("a.opened_date") < "2025-10-21")
    .select(F.col("a.id"), F.col("a.status"), F.col("a.account_type"))
    .orderBy(F.col("id").desc())
)
