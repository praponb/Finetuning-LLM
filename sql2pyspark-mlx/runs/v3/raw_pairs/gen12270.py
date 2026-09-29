from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("plan") == "enterprise") & F.col("start_date").between("2023-07-01", "2025-02-01") & F.col("plan").isin(["basic", "standard", "premium"]))
    .groupBy("customer_id", F.quarter("end_date").alias("end_quarter"))
    .agg(F.sort_array(F.collect_set("status")).alias("status_list"))
)
