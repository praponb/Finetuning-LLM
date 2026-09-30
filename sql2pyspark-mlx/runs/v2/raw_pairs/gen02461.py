from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type").isin(["CREDIT", "LOAN", "CHECKING"]))
    .select("opened_date", "customer_id")
    .orderBy("customer_id")
)
