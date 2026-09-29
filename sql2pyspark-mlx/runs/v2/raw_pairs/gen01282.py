from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type").isin(["CREDIT", "SAVINGS", "LOAN"]))
    .select("status")
    .intersect(df_payments.select("status"))
    .orderBy(F.col("status").desc())
)
