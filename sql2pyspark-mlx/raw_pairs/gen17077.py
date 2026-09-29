from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isNotNull())
    .agg(F.min("opened_date").alias("first_opened_date"))
)
