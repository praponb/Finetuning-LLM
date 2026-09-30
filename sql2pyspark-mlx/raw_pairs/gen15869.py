from pyspark.sql import functions as F

valid_accounts = (
    df_accounts
    .filter((F.year("opened_date") >= 2025) | F.col("customer_id").isin([11, 20, 39]))
    .withColumn("opened_date_plus_months", F.add_months("opened_date", 6))
)

agg = (
    valid_accounts
    .groupBy(F.year("opened_date").alias("opened_year"))
    .agg(F.round(F.avg("balance"), 2).alias("avg_balance"))
)

result = agg
