from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("end_date", "id", "subscription_id").rowsBetween(-5, Window.currentRow)
w_part = Window.partitionBy("plan").orderBy("subscription_id", "id", "subscription_id")
w3 = Window.partitionBy("status")
w_grp = Window.partitionBy("subscription_plan").orderBy(F.col("subscription_id_share").asc_nulls_first())
w5 = Window.partitionBy("customer_id").orderBy("next_monthly_fee", "id", "subscription_id").rowsBetween(-2, -1)

matched = (
    df_subscriptions
    .alias("e")
    .join(df_subscriptions.alias("sub"), (F.col("e.customer_id") == F.col("sub.customer_id")) & (F.col("e.end_date") >= F.col("sub.start_date")) & (F.col("e.end_date") <= F.date_add(F.col("sub.end_date"), 3)), "inner")
    .select(
        "e.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan").alias("subscription_plan"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee"),
    )
)

enriched = (
    matched
    .select(
        "*",
        F.avg("subscription_id").over(w).alias("subscription_id_avg_5"),
        F.lead("monthly_fee", 1).over(w_part).alias("next_monthly_fee"),
        F.round((F.col("subscription_id") / F.sum("subscription_id").over(w3)), 4).alias("subscription_id_share"),
    )
)

with_windows = (
    enriched
    .withColumn("subscription_id_share_dense_rank", F.dense_rank().over(w_grp))
    .withColumn("subscription_monthly_fee_share", F.round((F.col("subscription_monthly_fee") / F.sum("subscription_monthly_fee").over(w3)), 4))
    .withColumn("subscription_id_avg_2_prior", F.avg("subscription_id").over(w5))
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("subscription_id_avg_2_prior") >= 10, "HIGH").otherwise("LOW").alias("subscription_id_avg_2_prior_tier"),
        F.date_trunc("YEAR", "end_date").alias("year_start"),
        F.when(F.col("next_monthly_fee") >= 230, "LARGE").otherwise("SMALL").alias("next_monthly_fee_tier"),
    )
    .agg(
        F.percentile_approx("subscription_id_avg_5", 0.5).alias("subscription_id_avg_5_p50"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.max(F.when(F.col("subscription_plan") == "enterprise", F.col("subscription_monthly_fee"))).alias("enterprise_max_subscription_monthly_fee"),
    )
)
