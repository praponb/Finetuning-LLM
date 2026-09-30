from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(F.date_trunc("MONTH", "opened_date").alias("opened_month"), "status")
    .agg(
        F.min("opened_date").alias("min_opened_date"),
        F.sort_array(F.collect_set("account_type")).alias("account_type_list"),
        F.max("opened_date").alias("last_opened_date"),
    )
)
