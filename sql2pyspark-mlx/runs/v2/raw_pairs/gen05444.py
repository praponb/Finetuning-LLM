from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("balance") <= 40000) & F.col("customer_id").isNull())
    .select(
        "account_type",
        "opened_date",
        F.date_format("opened_date", "MMM yyyy").alias("opened_date_label"),
    )
)
