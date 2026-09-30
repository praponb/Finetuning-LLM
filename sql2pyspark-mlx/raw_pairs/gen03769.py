from pyspark.sql import functions as F

src = (
    df_subscriptions
    .filter((F.year("end_date") >= 2025) & ((F.col("plan") == "enterprise") | (F.col("plan") != "premium")))
)

subscriptions_stats = (
    src
    .groupBy(F.quarter("end_date").alias("end_quarter"))
    .agg(F.sort_array(F.collect_set("plan")).alias("plan_list"))
)

result = (
    subscriptions_stats
    .select("end_quarter", "plan_list")
    .orderBy(F.col("end_quarter").desc())
)
