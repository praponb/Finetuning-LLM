from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isin([38, 39]))
    .select(
        "id",
        "customer_id",
        "account_type",
        F.date_format("opened_date", "yyyy-MM-dd").alias("opened_date_label"),
        F.datediff(F.current_date(), "opened_date").alias("days_since_opened_date"),
    )
    .orderBy(F.col("customer_id").desc(), "opened_date_label")
)
