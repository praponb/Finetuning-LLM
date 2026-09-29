from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("opened_date") >= "2024-03-01") | F.col("account_type").isin(["CHECKING", "CREDIT", "LOAN"]))
    .withColumn("status_group", F.when(F.col("status").isin(["CLOSED", "OPEN"]), "Tier 1").otherwise("Other"))
    .withColumn("opened_date_label", F.date_format("opened_date", "MMM yyyy"))
)
