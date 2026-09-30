from pyspark.sql import functions as F

result = (
    df_customers
    .select("city")
    .subtract(
        df_accounts
        .filter(F.col("account_type").isin(["SAVINGS", "LOAN", "CHECKING"]))
        .select("status")
    )
)
