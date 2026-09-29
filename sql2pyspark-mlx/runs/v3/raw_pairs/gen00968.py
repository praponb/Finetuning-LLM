from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type") != "SAVINGS")
    .select("account_type", "customer_id")
    .intersect(
        df_accounts
        .filter(F.col("account_type").isin(["CHECKING", "CREDIT"]))
        .select("account_type", "customer_id")
    )
    .orderBy("customer_id")
)
