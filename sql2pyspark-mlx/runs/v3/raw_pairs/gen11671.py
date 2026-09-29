from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status").isNotNull() & (F.col("opened_date") > "2023-11-15"))
    .groupBy("account_type")
    .agg(
        F.round(F.stddev("balance"), 2).alias("stddev_balance"),
        F.min("opened_date").alias("first_opened_date"),
    )
    .orderBy("first_opened_date")
)
