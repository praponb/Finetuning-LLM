from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type").isin(["LOAN", "CREDIT", "CHECKING"]))
    .select("id", "customer_id")
    .unionByName(
        df_accounts
        .filter(F.col("customer_id").isNotNull())
        .select("id", "customer_id")
    )
    .orderBy("id")
)
