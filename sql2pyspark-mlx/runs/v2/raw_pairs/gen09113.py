from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("status", F.date_format("opened_date", "yyyy-MM").alias("opened_month"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
)
