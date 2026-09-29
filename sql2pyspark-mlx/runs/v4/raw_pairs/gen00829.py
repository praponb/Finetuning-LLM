from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id") == 19)
    .select("id", "customer_id", "opened_date")
    .intersect(
        df_accounts
        .filter(F.col("customer_id").isNull())
        .select("id", "customer_id", "opened_date")
    )
)
