from pyspark.sql import functions as F

valid_subscriptions = (
    df_subscriptions
    .filter(~(F.col("monthly_fee").between(120, 150) & ~F.col("status").isin(["EXPIRED", "ACTIVE", "CANCELLED"])))
    .withColumn("end_date_day", F.dayofmonth("end_date"))
)

agg = (
    valid_subscriptions
    .groupBy(F.quarter("start_date").alias("start_quarter"))
    .agg(
        F.count("*").alias("num_subscriptions"),
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
    )
)

result = (
    agg
    .filter(F.col("num_subscriptions") >= 7)
    .select("start_quarter", "num_subscriptions", "plan_list")
)
