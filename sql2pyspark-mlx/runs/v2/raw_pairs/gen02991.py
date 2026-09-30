from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("account_type") == "CREDIT") & F.month("opened_date").isin([1, 5, 7]))
    .select(
        "id",
        "customer_id",
        "opened_date",
        F.initcap(F.trim("account_type")).alias("account_type_title"),
    )
)
