from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status") == "FROZEN")
    .groupBy("account_type", "status")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("account_type"))).alias("account_type_values"),
        F.min("opened_date").alias("min_opened_date"),
        F.count("*").alias("account_count"),
    )
    .filter(F.col("account_count") >= 4)
    .orderBy("account_type")
)
