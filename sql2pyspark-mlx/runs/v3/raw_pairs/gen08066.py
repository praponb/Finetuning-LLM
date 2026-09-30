from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type") != "CHECKING")
    .select("id", "account_type")
    .unionByName(
        df_accounts
        .filter(F.month("opened_date").isin([1, 3, 7]))
        .select("id", "account_type")
    )
)
