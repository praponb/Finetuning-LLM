from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(F.date_format("opened_date", "yyyy-MM").alias("opened_month"))
    .agg(
        F.max("opened_date").alias("latest_opened_date"),
        (F.max("balance") - F.min("balance")).alias("balance_range"),
    )
    .filter(F.col("balance_range") > 70)
    .orderBy(F.col("balance_range").desc())
)
