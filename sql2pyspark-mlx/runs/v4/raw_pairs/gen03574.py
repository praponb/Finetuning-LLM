from pyspark.sql import functions as F

filtered = (
    df_accounts
    .filter(F.col("opened_date") >= F.lit("2023-04-28").cast("date"))
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.trim("status").alias("clean_status"),
        F.concat("status", F.lit(" - "), "account_type").alias("status_account_type"),
    )
)

result = (
    filtered
    .groupBy("customer_id")
    .agg(
        F.sort_array(F.collect_set("account_type")).alias("account_type_list"),
        F.stddev_samp("balance").alias("balance_stddev"),
    )
)
