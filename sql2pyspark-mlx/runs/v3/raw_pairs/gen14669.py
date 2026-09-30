from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance").between(16000, 24000) & F.col("status").isin(["CLOSED", "FROZEN"]))
    .select(
        "id",
        "opened_date",
        "customer_id",
        F.date_sub("opened_date", 28).alias("opened_date_minus_28d"),
    )
)
