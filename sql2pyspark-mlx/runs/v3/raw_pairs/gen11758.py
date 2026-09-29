from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy("status", F.quarter("end_date").alias("end_quarter"))
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.count("monthly_fee").alias("monthly_fee_count"),
    )
    .filter(F.col("monthly_fee_count") < 6)
    .select("status", "end_quarter", "status_list", "monthly_fee_count")
)
