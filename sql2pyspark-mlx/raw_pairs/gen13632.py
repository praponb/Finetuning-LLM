from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("status")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("account_type") == "CREDIT", 1).otherwise(0))) / F.count("*")), 1).alias("pct_credit"),
        F.min("opened_date").alias("first_opened_date"),
    )
    .filter(F.col("pct_credit") > 61)
    .select("status", "pct_credit", "first_opened_date")
    .orderBy(F.col("pct_credit").desc())
)
