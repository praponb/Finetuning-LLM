from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status").isin(["OPEN", "FROZEN"]) & F.month("opened_date").isin([3, 7, 12]) & ~F.col("account_type").isin(["LOAN", "CHECKING"]))
    .select("id", "customer_id", "balance")
    .orderBy(F.col("customer_id").desc())
)
