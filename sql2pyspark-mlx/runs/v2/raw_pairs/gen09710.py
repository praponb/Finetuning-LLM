from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status") == "FROZEN")
    .select(
        "id",
        "opened_date",
        "status",
        "account_type",
        F.dayofweek("opened_date").alias("opened_date_dayofweek"),
    )
    .orderBy("account_type", F.col("opened_date").desc())
)
