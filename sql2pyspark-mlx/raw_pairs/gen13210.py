from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(~F.col("account_type").isin(["LOAN", "CREDIT", "CHECKING"]) & ((F.col("balance") <= 61000) | F.col("opened_date").between("2023-10-21", "2025-09-01")))
    .select(
        "id",
        "balance",
        "account_type",
        "status",
        F.substring("account_type", 1, 4).alias("account_type_prefix"),
    )
    .orderBy("account_type_prefix", "id")
)
