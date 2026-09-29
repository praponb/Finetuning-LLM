from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isin([6, 26, 40]))
    .select("status")
    .intersect(
        df_subscriptions
        .filter(F.col("start_date") > "2023-08-30")
        .select("status")
    )
    .orderBy("status")
)
