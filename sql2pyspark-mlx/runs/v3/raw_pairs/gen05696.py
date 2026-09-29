from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("account_type") == "CHECKING") & F.col("status").isin(["CLOSED", "FROZEN"]) & F.month("opened_date").isin([2, 6, 9]))
    .withColumn("account_type_title", F.initcap(F.trim("account_type")))
    .orderBy(F.col("opened_date").desc())
)
