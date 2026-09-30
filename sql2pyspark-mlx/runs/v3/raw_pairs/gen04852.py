from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("account_type") != "CHECKING") & F.col("customer_id").isin([26, 36]))
    .groupBy(F.date_format("opened_date", "yyyy-MM").alias("opened_month"))
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.round(((100 * F.sum(F.when(F.col("account_type") == "LOAN", 1).otherwise(0))) / F.count("*")), 1).alias("pct_loan"),
    )
    .orderBy("opened_month", F.col("pct_loan").desc())
)
