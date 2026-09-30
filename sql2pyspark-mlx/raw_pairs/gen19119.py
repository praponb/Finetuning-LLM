from pyspark.sql import functions as F

valid_accounts = (
    df_accounts
    .filter((F.col("opened_date") <= "2024-03-01") & ((F.col("opened_date") <= "2025-08-01") | F.col("status").isin(["OPEN", "FROZEN"])))
)

agg = (
    valid_accounts
    .groupBy(F.month("opened_date").alias("opened_month"))
    .agg(F.max("opened_date").alias("last_opened_date"))
)

result = agg.select("opened_month", "last_opened_date")
