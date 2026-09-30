from pyspark.sql import functions as F

filtered = (
    df_accounts
    .filter(~F.col("status").isin(["CLOSED", "FROZEN"]))
    .withColumn("opened_date_dayofweek", F.dayofweek("opened_date"))
)

result = (
    filtered
    .withColumn("year_start", F.date_trunc("year", "opened_date"))
    .withColumn("opened_date_dayofweek_tier", F.when(F.col("opened_date_dayofweek") >= 67, "LARGE").otherwise("SMALL"))
    .cube("year_start", "opened_date_dayofweek_tier", "status")
    .agg(
        F.sum("opened_date_dayofweek").alias("total_opened_date_dayofweek"),
        F.percentile_approx("opened_date_dayofweek", [0.25, 0.5, 0.75]).alias("opened_date_dayofweek_quartiles"),
        F.countDistinct("status").alias("distinct_status"),
        F.round(F.avg("balance"), 2).alias("avg_balance"),
        F.size(F.collect_set("account_type")).alias("num_account_types"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy("year_start")
)
