from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(F.date_format("opened_date", "yyyy-MM").alias("opened_month"))
    .agg(
        F.sum("balance").alias("total_balance"),
        F.min("opened_date").alias("first_opened_date"),
        F.concat_ws(", ", F.sort_array(F.collect_set("account_type"))).alias("account_type_values"),
    )
    .orderBy(F.col("account_type_values").desc())
)
