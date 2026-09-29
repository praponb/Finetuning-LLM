from pyspark.sql import functions as F

max_opened_date = df_accounts.filter(F.col("opened_date") > "2024-04-01").agg(F.max("opened_date")).first()[0]

result = (
    df_accounts
    .filter(F.col("opened_date") == max_opened_date)
    .select("id", "opened_date", "customer_id", "balance")
    .orderBy(F.col("balance").desc(), "id")
    .limit(10)
)
