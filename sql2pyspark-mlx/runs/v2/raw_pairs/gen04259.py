from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("customer_id").isin([4, 38]) & F.col("status").isNotNull()) | F.col("status").isNotNull())
    .select(
        "id",
        "customer_id",
        "opened_date",
        "balance",
        F.date_sub("opened_date", 70).alias("opened_date_minus_70d"),
    )
)
