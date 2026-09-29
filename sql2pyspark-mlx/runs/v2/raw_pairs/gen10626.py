from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance") >= 73000)
    .select(
        "id",
        "opened_date",
        "account_type",
        "customer_id",
        F.initcap(F.trim("account_type")).alias("account_type_title"),
    )
    .orderBy("account_type_title", "id")
    .limit(3)
)
