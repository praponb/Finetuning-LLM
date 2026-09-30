from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("start_date").desc_nulls_last(), "id")
w2 = Window.partitionBy("customer_id").orderBy("start_date", "id")
w3 = Window.partitionBy("customer_id").orderBy("monthly_fee_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "start_date",
        "monthly_fee",
        "status",
        F.round("monthly_fee", 1).alias("monthly_fee_rounded"),
    )
)

deduped = (
    valid
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

metrics = (
    deduped
    .withColumn("next_monthly_fee_rounded", F.lead("monthly_fee_rounded", 1).over(w2))
    .withColumn("first_known_monthly_fee_rounded", F.first("monthly_fee_rounded", ignorenulls=True).over(w3))
)

attributed = (
    metrics
    .alias("a")
    .join(df_subscriptions.alias("sub"), (F.col("a.customer_id") == F.col("sub.customer_id")) & (F.col("a.start_date") >= F.col("sub.start_date")) & (F.col("a.start_date") < F.col("sub.end_date") + F.expr("INTERVAL 14 DAYS")), "left")
    .select(
        "a.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee"),
    )
)

stats = (
    attributed
    .groupBy(F.date_trunc("MONTH", "start_date").alias("activity_month"))
    .agg(
        F.percentile_approx("monthly_fee", 0.9).alias("monthly_fee_p90"),
        F.count(F.when(F.col("status") == "EXPIRED", 1)).alias("expired_rows"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.sum(F.when(F.col("status") == "CANCELLED", F.col("first_known_monthly_fee_rounded")).otherwise(0)).alias("cancelled_first_known_monthly_fee_rounded"),
        F.size(F.collect_set("status")).alias("num_status_2"),
    )
)

result = (
    stats
    .select(
        "activity_month",
        "monthly_fee_p90",
        "expired_rows",
        "num_status",
        "cancelled_first_known_monthly_fee_rounded",
        "num_status_2",
    )
    .orderBy("expired_rows")
)
