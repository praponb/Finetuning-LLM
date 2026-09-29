from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("signup_date").isNotNull())
    .select("signup_date")
    .intersect(
        df_accounts
        .filter(F.col("opened_date").isNotNull())
        .select("opened_date")
    )
)
