from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.month("opened_date").isin([1, 2, 8]))
    .select("account_type")
    .unionByName(df_accounts.filter(F.col("status") == "CLOSED").select("account_type"))
    .distinct()
)
