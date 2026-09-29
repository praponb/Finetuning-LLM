from pyspark.sql import functions as F

filtered_subscriptions = df_subscriptions.filter(F.col("monthly_fee") >= 110)

summary = (
    filtered_subscriptions
    .groupBy(
        F.when(F.col("status").isin(["ACTIVE", "EXPIRED"]), "group_a").otherwise("other").alias("status_group"),
    )
    .agg(F.sort_array(F.collect_set("status")).alias("status_list"))
)

result = (
    summary
    .select("status_group", "status_list")
    .orderBy(F.col("status_group").desc())
)
