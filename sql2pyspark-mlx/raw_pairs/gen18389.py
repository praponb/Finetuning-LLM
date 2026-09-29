from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status") != "OPEN")
    .select("id", "opened_date", F.ceil("balance").alias("balance_whole"))
    .orderBy("opened_date", "id")
    .limit(10)
)
