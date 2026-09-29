from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("opened_date") < "2024-09-21") & (F.col("account_type") == "CHECKING"))
    .select("id", "opened_date", "balance")
    .orderBy(F.col("balance").desc())
)
