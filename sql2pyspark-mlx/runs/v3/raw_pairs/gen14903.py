from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isNotNull())
    .select("opened_date", "customer_id")
    .subtract(
        df_accounts
        .filter(F.col("status").isNotNull())
        .select("opened_date", "customer_id")
    )
)
