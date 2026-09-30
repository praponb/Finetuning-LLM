from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.month("opened_date").isin([5, 9, 12]) & F.col("customer_id").isin([19, 25, 27, 36]) & (F.col("status") == "CLOSED"))
    .select(
        "id",
        "balance",
        "customer_id",
        F.coalesce("balance", F.lit(0)).alias("balance_filled"),
    )
)
