from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(~F.col("status").isin(["CLOSED", "OPEN"]))
    .select("id", "status")
    .orderBy(F.col("id").desc())
)
