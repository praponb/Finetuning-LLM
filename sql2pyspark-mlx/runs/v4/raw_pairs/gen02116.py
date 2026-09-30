from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .filter(F.col("a.account_type").isin(["CHECKING", "CREDIT", "LOAN"]) | ~F.col("a.account_type").isin(["LOAN", "CHECKING"]))
    .select(F.col("t.id"), F.col("t.amount"), F.col("a.status"))
    .orderBy("id", "amount")
)
