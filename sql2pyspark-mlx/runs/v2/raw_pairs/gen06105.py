from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id") == 4)
    .select("customer_id", "opened_date")
    .unionByName(
        df_accounts
        .filter(F.month("opened_date").isin([6, 8, 9]))
        .select("customer_id", "opened_date")
    )
)
