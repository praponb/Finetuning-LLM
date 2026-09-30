from pyspark.sql import functions as F

result = (
    df_accounts
    .select(
        "id",
        "balance",
        "opened_date",
        "status",
        F.initcap(F.trim("account_type")).alias("account_type_title"),
    )
    .orderBy("balance", "id")
    .limit(5)
)
