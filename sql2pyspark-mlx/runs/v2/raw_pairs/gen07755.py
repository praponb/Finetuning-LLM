from pyspark.sql import functions as F

src = df_accounts.filter(F.col("status") == "CLOSED")

opened_year_summary = (
    src
    .groupBy(F.year("opened_date").alias("opened_year"))
    .agg(F.max("balance").alias("max_balance"))
)

result = (
    opened_year_summary
    .filter(F.col("max_balance") < 38000)
    .select("opened_year", "max_balance")
)
