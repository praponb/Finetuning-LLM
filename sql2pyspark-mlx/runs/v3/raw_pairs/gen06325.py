from pyspark.sql import functions as F

max_opened_date = df_accounts.filter(F.col("balance") < 52000).agg(F.max("opened_date")).first()[0]

result = (
    df_accounts
    .filter(F.col("opened_date") == max_opened_date)
    .select("id", "opened_date")
)
