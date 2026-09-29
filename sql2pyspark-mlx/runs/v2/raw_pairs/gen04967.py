from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("end_date") < "2025-09-22")
    .groupBy("plan", "status")
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.count(F.when(F.col("status") == "CANCELLED", 1)).alias("cancelled_count"),
    )
    .filter(F.col("cancelled_count") >= 7)
    .select("plan", "status", "status_list", "cancelled_count")
)
