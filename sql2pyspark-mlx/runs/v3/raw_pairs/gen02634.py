from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("status") != "OPEN") & F.col("opened_date").between("2024-02-15", "2024-04-11") & F.col("customer_id").isNotNull())
    .select(
        "id",
        "account_type",
        "balance",
        "customer_id",
        F.substring("account_type", 1, 4).alias("account_type_prefix"),
    )
    .orderBy(F.col("account_type").desc())
)
