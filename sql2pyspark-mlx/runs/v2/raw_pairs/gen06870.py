from pyspark.sql import functions as F

max_opened_date = df_accounts.filter(F.col("customer_id").isNull()).agg(F.max("opened_date")).first()[0]

result = (
    df_accounts
    .filter(F.col("opened_date") == max_opened_date)
    .select("id", "opened_date", "customer_id")
    .orderBy(F.col("customer_id").desc())
)
