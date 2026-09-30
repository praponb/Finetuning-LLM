from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status").isin(["FROZEN", "CLOSED"]))
    .orderBy(F.col("id").desc(), "opened_date")
)
