from pyspark.sql import functions as F

result = (
    df_accounts
    .select(
        "id",
        "balance",
        "status",
        F.dayofmonth("opened_date").alias("opened_date_day"),
        F.initcap(F.trim("account_type")).alias("account_type_title"),
    )
    .orderBy("status", "account_type_title")
)
