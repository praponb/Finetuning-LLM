from pyspark.sql import functions as F

valid = (
    df_accounts
    .filter(F.col("opened_date") > F.to_date(F.lit("2023-04-05")))
    .select(
        "id",
        "customer_id",
        "account_type",
        "status",
        F.floor("balance").alias("balance_whole"),
        F.datediff(F.current_date(), "opened_date").alias("days_since_opened_date"),
        F.coalesce("status", F.lit("NONE")).alias("resolved_status"),
    )
)

result = (
    valid
    .withColumn("days_since_opened_date_tier", F.when(F.col("days_since_opened_date") >= 71, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["days_since_opened_date_tier", "account_type"], ["days_since_opened_date_tier"], []],
        "days_since_opened_date_tier",
        "account_type",
    )
    .agg(
        F.percentile_approx("balance_whole", 0.5).alias("balance_whole_p50"),
        F.count(F.when(F.col("status") == "FROZEN", 1)).alias("frozen_count"),
        F.sum(F.when(F.col("account_type") == "CHECKING", F.col("days_since_opened_date"))).alias("checking_total_days_since_opened_date"),
        F.sum(F.when(F.col("status") == "CLOSED", F.col("days_since_opened_date")).otherwise(0)).alias("closed_days_since_opened_date"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.grouping("days_since_opened_date_tier").alias("gid"),
    )
    .orderBy(F.col("balance_whole_p50").desc())
)
