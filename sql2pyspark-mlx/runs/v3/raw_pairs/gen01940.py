from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status").isin(["CLOSED", "OPEN", "FROZEN"]))
    .withColumn("opened_date_plus_67d", F.date_add("opened_date", 67))
    .orderBy(F.col("balance").desc())
)
