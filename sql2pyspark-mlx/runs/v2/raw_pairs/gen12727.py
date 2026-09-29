from pyspark.sql import functions as F

src = df_accounts.filter(F.col("balance") <= 75000)

accounts_stats = (
    src
    .groupBy("status")
    .agg(F.min("opened_date").alias("first_opened_date"))
)

result = (
    accounts_stats
    .select("status", "first_opened_date")
    .orderBy(F.col("status").desc())
)
