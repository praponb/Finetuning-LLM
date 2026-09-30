from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type").isin(["CHECKING", "LOAN"]))
    .select("status")
    .subtract(df_payments.filter(F.col("amount").isNotNull()).select("status"))
    .orderBy("status")
)
