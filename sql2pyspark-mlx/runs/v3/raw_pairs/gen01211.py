from pyspark.sql import functions as F

result = (
    df_accounts
    .select("id", "balance", F.date_sub("opened_date", 7).alias("opened_date_minus_7d"))
)
