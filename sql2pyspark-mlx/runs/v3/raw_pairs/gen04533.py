from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance").between(68000, 76000))
    .select("customer_id", "status")
    .subtract(
        df_accounts
        .filter(F.col("account_type").isin(["CHECKING", "CREDIT"]))
        .select("customer_id", "status")
    )
    .orderBy("status")
)
