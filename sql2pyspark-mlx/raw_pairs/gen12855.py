from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(F.year("opened_date").alias("opened_year"))
    .agg(
        F.max("opened_date").alias("latest_opened_date"),
        F.round(((100 * F.sum(F.when(F.col("account_type") == "SAVINGS", 1).otherwise(0))) / F.count("*")), 1).alias("pct_savings"),
    )
    .filter(F.col("pct_savings") < 28)
    .orderBy(F.col("pct_savings").desc(), "opened_year")
    .limit(50)
)
