from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isNull() | (F.col("account_type") == "SAVINGS"))
    .groupBy("account_type")
    .agg(
        F.max("opened_date").alias("last_opened_date"),
        F.countDistinct("balance").alias("unique_account_types"),
    )
    .orderBy(F.col("account_type").desc())
)
