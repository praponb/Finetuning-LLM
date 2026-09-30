from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .filter(~F.col("t.merchant").isin(["Lazada", "Shell", "Grab"]))
    .select(
        F.col("a.opened_date"),
        F.col("a.status"),
        F.col("t.merchant"),
        F.coalesce(F.col("t.amount"), F.lit(0)).alias("amount_filled"),
    )
    .orderBy(F.col("amount_filled").desc())
)
