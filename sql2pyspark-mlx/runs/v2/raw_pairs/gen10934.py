from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((~F.col("account_type").isin(["CHECKING", "SAVINGS", "LOAN"]) & (F.col("account_type") != "SAVINGS")) | F.col("opened_date").between("2024-04-01", "2024-10-05"))
    .withColumn("status_length", F.length("status"))
)
