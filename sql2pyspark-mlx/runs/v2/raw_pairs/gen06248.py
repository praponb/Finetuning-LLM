from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date").between("2023-04-01", "2023-08-01"))
    .select("id", "opened_date", "status")
)
