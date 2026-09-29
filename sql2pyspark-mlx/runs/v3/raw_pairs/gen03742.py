from pyspark.sql import functions as F

result = (
    df_accounts
    .select(
        "id",
        "account_type",
        "opened_date",
        "status",
        F.date_add("opened_date", 53).alias("opened_date_plus_53d"),
    )
    .orderBy("status", F.col("opened_date").desc())
)
