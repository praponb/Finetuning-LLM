from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isNull() | (F.col("status") == "CLOSED"))
    .agg(
        F.min("opened_date").alias("first_opened_date"),
        F.countDistinct("balance").alias("unique_customers"),
    )
    .orderBy("first_opened_date", "unique_customers")
)
