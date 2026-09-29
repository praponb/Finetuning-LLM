from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan", "customer_id").orderBy("monthly_fee")
w2 = Window.partitionBy("customer_id").orderBy(F.col("monthly_fee").desc(), "id")
w3 = Window.partitionBy("status").orderBy("rank_in_group", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

ranked = (
    df_subscriptions
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
)

ranked_subscriptions = (
    ranked
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
)

with_windows = (
    ranked_subscriptions
    .filter(F.col("start_date") <= "2024-12-01")
    .withColumn("running_max_monthly_fee", F.max("monthly_fee").over(w3))
    .withColumn("running_avg_monthly_fee", F.avg("monthly_fee").over(w3))
    .withColumn("running_min_monthly_fee", F.min("monthly_fee").over(w3))
)

with_dims = (
    with_windows
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "left")
    .select(
        "e.id",
        "e.customer_id",
        "e.start_date",
        "e.monthly_fee",
        "e.status",
        "e.dedup_rank",
        "e.rank_in_group",
        "e.running_avg_monthly_fee",
        F.col("b.age"),
    )
)

daily = (
    with_dims
    .groupBy("status")
    .agg(
        F.percentile_approx("running_avg_monthly_fee", 0.5).alias("running_avg_monthly_fee_p50"),
        F.sum(F.when(F.col("status") == "EXPIRED", F.col("age")).otherwise(0)).alias("expired_age"),
    )
)

result = (
    daily
    .select(
        "status",
        "expired_age",
        F.when(F.col("status").rlike("^(PAUSED|CANCELLED)$"), "OTHER_ZONE").otherwise("GROUP_B").alias("status_zone"),
    )
)
