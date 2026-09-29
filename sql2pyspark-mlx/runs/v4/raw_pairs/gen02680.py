from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type").isin(["CHECKING", "LOAN", "CREDIT"]) & ((F.col("status") == "OPEN") | (F.col("status") == "OPEN")))
    .orderBy(F.col("opened_date").desc())
)
