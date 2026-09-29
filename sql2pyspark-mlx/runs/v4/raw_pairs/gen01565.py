from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type").isin(["SAVINGS", "CHECKING", "LOAN"]))
    .select("id", "customer_id", "status")
    .unionByName(
        df_accounts
        .filter(F.year("opened_date") == 2024)
        .select("id", "customer_id", "status")
    )
    .orderBy("status")
)
