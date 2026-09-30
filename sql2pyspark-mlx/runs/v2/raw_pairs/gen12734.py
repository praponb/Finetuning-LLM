from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date").between("2023-08-01", "2023-09-01") & F.month("opened_date").isin([1, 4, 7]))
    .withColumn("status_title", F.initcap(F.trim("status")))
    .orderBy(F.col("account_type").desc())
)
