from pyspark.sql import functions as F

filtered_accounts = (
    df_accounts
    .filter((F.col("account_type") == "CREDIT") & (F.col("opened_date") < "2025-06-16"))
    .withColumn("days_since_opened_date", F.datediff(F.current_date(), "opened_date"))
)

agg = (
    filtered_accounts
    .groupBy(F.date_trunc("MONTH", "opened_date").alias("opened_month"))
    .agg(
        F.sum("days_since_opened_date").alias("total_days_since_opened_date"),
        F.sort_array(F.collect_set("account_type")).alias("account_type_list"),
    )
)

result = (
    agg
    .filter(F.col("total_days_since_opened_date") < 350)
    .select("opened_month", "total_days_since_opened_date", "account_type_list")
    .orderBy(F.col("total_days_since_opened_date").desc(), "opened_month")
    .limit(1)
)
