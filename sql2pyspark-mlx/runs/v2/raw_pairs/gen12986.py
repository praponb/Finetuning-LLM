from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date").between("2023-03-10", "2024-05-01") & (F.col("account_type").isin(["CHECKING", "SAVINGS"]) | (F.col("status") == "OPEN")))
)
