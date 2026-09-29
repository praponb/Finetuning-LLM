from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.month("opened_date").isin([1, 7, 12]))
    .select(
        "customer_id",
        F.date_trunc("month", "opened_date").alias("opened_date_period"),
    )
)
