from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("opened_date") <= "2024-12-24") | ~F.col("status").isin(["OPEN", "CLOSED"]))
    .select("account_type")
)
