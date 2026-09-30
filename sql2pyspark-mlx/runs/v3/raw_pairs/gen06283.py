from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type") == "SAVINGS")
    .groupBy(F.quarter("opened_date").alias("opened_quarter"))
    .agg(
        F.countDistinct("balance").alias("unique_statuss"),
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
    .filter(F.col("unique_statuss") < 9)
    .orderBy("unique_statuss", "opened_quarter")
    .limit(50)
)
