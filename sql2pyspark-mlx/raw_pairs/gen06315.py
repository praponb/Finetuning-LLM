from pyspark.sql import functions as F

filtered_accounts = (
    df_accounts
    .filter(((F.col("account_type") == "CREDIT") & (F.col("status") == "OPEN")) | F.col("status").isin(["OPEN", "CLOSED", "FROZEN"]))
)

summary = (
    filtered_accounts
    .groupBy("status", "account_type")
    .agg(
        F.max("opened_date").alias("latest_opened_date"),
        F.sort_array(F.collect_set("account_type")).alias("account_type_list"),
    )
)

result = summary.orderBy("account_type")
