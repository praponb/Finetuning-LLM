from pyspark.sql import functions as F

account_summary = (
    df_transactions
    .groupBy("account_id")
    .agg(F.sum(F.when(F.col("merchant") == "Grab", 1).otherwise(0)).alias("grab_count"))
)

result = (
    df_accounts.alias("a")
    .join(account_summary.alias("A"), F.col("a.id") == F.col("A.account_id"), "left")
    .filter(F.col("A.grab_count") >= 66)
    .select(F.col("a.opened_date"), F.col("a.customer_id"), F.col("A.grab_count"))
    .orderBy("grab_count")
)
