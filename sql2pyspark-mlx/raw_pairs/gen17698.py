from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(F.year("opened_date").alias("opened_year"), "status")
    .agg(F.max("opened_date").alias("last_opened_date"))
)
