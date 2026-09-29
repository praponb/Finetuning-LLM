from pyspark.sql import functions as F

recent_accounts = (
    df_accounts
    .filter(~F.col("account_type").isin(["LOAN", "CHECKING", "SAVINGS"]) & (F.col("opened_date") <= "2023-03-16"))
)

agg = (
    recent_accounts
    .groupBy("status", F.year("opened_date").alias("opened_year"))
    .agg(F.count(F.when(F.col("status") == "FROZEN", 1)).alias("frozen_count"))
)

result = (
    agg
    .filter(F.col("frozen_count") < 8)
    .select("status", "opened_year")
    .orderBy(F.col("opened_year").desc())
)
