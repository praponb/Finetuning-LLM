from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(F.date_format("start_date", "yyyy-MM").alias("start_month"))
    .agg(
        F.count("*").alias("num_subscriptions"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
    .filter(F.col("num_subscriptions") >= 3)
    .orderBy(F.col("num_subscriptions").desc())
)
