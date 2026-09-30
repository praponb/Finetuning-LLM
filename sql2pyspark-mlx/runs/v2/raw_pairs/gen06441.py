from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("opened_date") > "2023-08-16") & F.col("customer_id").isin([13, 35]))
    .agg(F.min("opened_date").alias("first_opened_date"))
    .orderBy("first_opened_date")
)
