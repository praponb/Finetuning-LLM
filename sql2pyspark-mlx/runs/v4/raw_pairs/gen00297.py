from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status").isin(["OPEN", "FROZEN"]))
    .select(
        "opened_date",
        "account_type",
        F.abs("balance").alias("abs_balance"),
        F.initcap(F.trim("status")).alias("status_title"),
    )
)
