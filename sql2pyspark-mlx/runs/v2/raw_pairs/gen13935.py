from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("status")
    .agg(
        F.max("opened_date").alias("last_opened_date"),
        (F.max("balance") - F.min("balance")).alias("balance_range"),
        F.min("opened_date").alias("first_opened_date"),
    )
    .filter(F.col("balance_range") > 51)
    .orderBy("balance_range", "status")
    .limit(3)
)
