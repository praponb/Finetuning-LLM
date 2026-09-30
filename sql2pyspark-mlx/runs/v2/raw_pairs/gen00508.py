from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status").isNull())
    .groupBy("account_type", F.date_trunc("MONTH", "opened_date").alias("opened_month"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("account_type"))).alias("account_type_values"),
        F.sum("balance").alias("total_balance"),
    )
    .orderBy("opened_month")
)
