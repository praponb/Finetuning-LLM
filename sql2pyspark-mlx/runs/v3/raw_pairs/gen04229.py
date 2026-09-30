from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type").isin(["SAVINGS", "LOAN", "CHECKING"]))
    .select(
        "id",
        "balance",
        "opened_date",
        F.when(F.col("account_type") == "CHECKING", 1).otherwise(0).alias("is_checking"),
    )
    .orderBy(F.col("opened_date").desc(), "balance")
)
