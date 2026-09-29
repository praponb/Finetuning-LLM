from pyspark.sql import functions as F

src = df_accounts.filter((F.col("balance") < 77000) | (F.col("balance") > 78000))

accounts_stats = (
    src
    .groupBy(F.year("opened_date").alias("opened_year"))
    .agg(F.min("balance").alias("min_balance"))
)

result = (
    accounts_stats
    .filter(F.col("min_balance") > 49000)
    .select("opened_year", "min_balance")
    .orderBy(F.col("min_balance").desc())
)
