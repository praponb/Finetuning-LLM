from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("status") == "FROZEN") & (F.col("opened_date") > "2024-05-01"))
    .agg(
        F.sum("balance").alias("total_balance"),
        F.concat_ws(", ", F.sort_array(F.collect_set("account_type"))).alias("account_type_values"),
    )
    .orderBy("total_balance")
)
