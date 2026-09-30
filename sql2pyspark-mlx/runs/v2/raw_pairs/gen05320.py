from pyspark.sql import functions as F

result = (
    df_accounts
    .select("status")
    .subtract(
        df_payments
        .filter(F.col("amount").between(980, 1250))
        .select("status")
    )
    .orderBy("status")
)
