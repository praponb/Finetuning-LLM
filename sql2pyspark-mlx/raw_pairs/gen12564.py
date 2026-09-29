from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("created_at", "id", "attr_key")
w2 = Window.partitionBy("customer_id").orderBy("created_at", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode("attributes").alias("attr_key", "value"),
    )
)

with_gaps = (
    exploded
    .withColumn("new_group_flag", F.when(F.lag("created_at").over(w).isNull() | (F.col("created_at") > F.lag("created_at").over(w) + F.expr("INTERVAL 20 MINUTES")), 1).otherwise(0))
)

sessions_carts = with_gaps.withColumn("session_no", F.sum("new_group_flag").over(w2))

attributed = (
    sessions_carts
    .alias("src")
    .join(df_subscriptions.alias("sub"), (F.col("src.customer_id") == F.col("sub.customer_id")) & F.col("src.created_at").between(F.col("sub.start_date"), F.col("sub.end_date")), "left")
    .select(
        "src.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

matched = (
    attributed
    .alias("src")
    .join(df_subscriptions.alias("sub"), (F.col("src.customer_id") == F.col("sub.customer_id")) & (F.col("src.created_at") >= F.col("sub.start_date")) & (F.col("src.created_at") < F.col("sub.end_date") + F.expr("INTERVAL 14 DAYS")), "left")
    .select(
        "src.*",
        F.col("sub.id").alias("subscription_id_2"),
        F.col("sub.plan").alias("subscription_plan"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee"),
    )
)

result = (
    matched
    .groupBy(
        F.when(F.col("monthly_fee") >= 230, "HIGH").otherwise("LOW").alias("monthly_fee_tier"),
        F.date_trunc("DAY", "created_at").alias("activity_day"),
    )
    .agg(
        F.sum("subscription_monthly_fee").alias("total_subscription_monthly_fee"),
        F.sum(F.when(F.col("subscription_plan") == "basic", F.col("subscription_id")).otherwise(0)).alias("basic_subscription_id"),
        F.percentile_approx("session_no", 0.9).alias("session_no_p90"),
    )
    .orderBy(F.col("monthly_fee_tier").asc_nulls_last())
)
