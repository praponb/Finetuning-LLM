from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type").isin(["CREDIT", "SAVINGS", "CHECKING"]) & F.month("opened_date").isin([3, 4, 12]))
    .select(
        "id",
        "status",
        "account_type",
        "opened_date",
        F.date_sub("opened_date", 78).alias("opened_date_minus_78d"),
    )
    .orderBy(F.col("id").desc())
)
