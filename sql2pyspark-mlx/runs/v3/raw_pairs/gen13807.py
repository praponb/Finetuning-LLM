from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("account_type", F.month("opened_date").alias("opened_month"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
)
