from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.month("opened_date").isin([3, 7, 12]))
    .select("id", "account_type")
    .intersect(
        df_accounts
        .filter(F.col("customer_id").isin([1, 24, 37]))
        .select("id", "account_type")
    )
)
