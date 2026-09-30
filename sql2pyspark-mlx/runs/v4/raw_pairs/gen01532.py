from pyspark.sql import functions as F

recent_accounts = df_accounts.filter(F.col("account_type") != "LOAN")

agg = (
    recent_accounts
    .groupBy(F.year("opened_date").alias("opened_year"))
    .agg(F.countDistinct("balance").alias("unique_statuss"))
)

result = agg.orderBy("opened_year")
