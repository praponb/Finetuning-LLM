from pyspark.sql import functions as F

result = (
    df_accounts
    .withColumn("activity_month", F.date_trunc("MONTH", "opened_date"))
    .withColumn("quarter_start", F.date_trunc("QUARTER", "opened_date"))
    .groupingSets(
        [["activity_month", "quarter_start"], ["activity_month"]],
        "activity_month",
        "quarter_start",
    )
    .agg(
        F.count("*").alias("event_count"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.size(F.collect_set("status")).alias("num_status_2"),
        F.count(F.when(F.col("status") == "OPEN", 1)).alias("open_rows"),
        F.sum(F.when(F.col("status") == "FROZEN", F.col("balance")).otherwise(0)).alias("frozen_balance"),
        F.count_if(F.col("account_type") == "CHECKING").alias("checking_events"),
        F.grouping_id().alias("grouping_level"),
    )
)
