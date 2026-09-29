from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("status", F.month("opened_date").alias("opened_month"))
    .agg(
        F.sum("balance").alias("total_balance"),
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
    .orderBy("status")
)
