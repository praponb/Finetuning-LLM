from pyspark.sql import functions as F

max_opened_date = df_accounts.filter(F.col("opened_date") >= "2023-09-01").agg(F.max("opened_date")).first()[0]

result = (
    df_accounts
    .filter(F.col("opened_date") > max_opened_date)
    .select("id", "opened_date", "customer_id")
    .orderBy(F.col("id").desc())
    .limit(1)
)
