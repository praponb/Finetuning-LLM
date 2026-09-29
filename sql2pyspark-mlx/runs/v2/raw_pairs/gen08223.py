from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type").isin(["CHECKING", "SAVINGS"]))
    .select("customer_id", "status")
    .intersect(
        df_accounts
        .filter(F.col("customer_id").isNull())
        .select("customer_id", "status")
    )
)
