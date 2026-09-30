from pyspark.sql import functions as F

result = (
    df_accounts
    .select(
        "status",
        "account_type",
        F.date_format("opened_date", "yyyy-MM-dd").alias("opened_date_label"),
    )
    .orderBy("account_type")
)
