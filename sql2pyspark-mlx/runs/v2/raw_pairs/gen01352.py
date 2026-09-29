from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date") < "2023-07-01")
    .groupBy("account_type")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.max("opened_date").alias("last_opened_date"),
        F.sum(F.when(F.col("status") == "FROZEN", 1).otherwise(0)).alias("frozen_count"),
    )
)
