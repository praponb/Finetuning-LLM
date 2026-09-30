from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("customer_id")
    .agg(
        F.min("opened_date").alias("first_opened_date"),
        (F.max("balance") - F.min("balance")).alias("balance_range"),
        F.round(F.stddev("balance"), 2).alias("stddev_balance"),
    )
    .orderBy(F.col("customer_id").desc())
    .limit(50)
)
