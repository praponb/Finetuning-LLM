from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(
        F.when(F.col("plan").isin(["standard", "enterprise"]), "core").otherwise("other").alias("plan_group"),
    )
    .agg(
        F.max("start_date").alias("latest_start_date"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
    .orderBy(F.col("latest_start_date").desc())
)
