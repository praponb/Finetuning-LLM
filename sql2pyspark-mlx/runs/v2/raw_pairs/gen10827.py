from pyspark.sql import functions as F

max_opened_date = df_accounts.agg(F.max("opened_date")).first()[0]

result = (
    df_accounts
    .filter(F.col("opened_date") > max_opened_date)
    .select("id", "opened_date", "status", "customer_id")
    .orderBy("status", F.col("customer_id").desc())
)
