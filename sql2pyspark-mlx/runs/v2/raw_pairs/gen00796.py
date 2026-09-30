from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isNull())
    .agg(
        F.min("opened_date").alias("min_opened_date"),
        F.min("opened_date").alias("first_opened_date"),
    )
    .orderBy(F.col("min_opened_date").desc(), "first_opened_date")
)
