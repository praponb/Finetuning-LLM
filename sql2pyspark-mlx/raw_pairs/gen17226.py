from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("end_date").desc())

filtered = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .withColumn("status_prefix", F.substring("status", 1, 3))
)

deduped = (
    filtered
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
)

attributed = (
    deduped
    .alias("e")
    .join(df_subscriptions.alias("sub"), (F.col("e.customer_id") == F.col("sub.customer_id")) & F.col("e.end_date").between(F.col("sub.start_date"), F.col("sub.end_date")), "left")
    .select(
        "e.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan").alias("subscription_plan"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee"),
    )
)

matched = (
    attributed
    .alias("a")
    .join(df_subscriptions.alias("sub"), (F.col("a.customer_id") == F.col("sub.customer_id")) & (F.col("a.end_date") >= F.col("sub.start_date")) & (F.col("a.end_date") <= F.date_add(F.col("sub.end_date"), 3)), "left")
    .select(
        "a.*",
        F.col("sub.id").alias("subscription_id_2"),
        F.col("sub.plan").alias("subscription_plan_2"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee_2"),
    )
)

result = (
    matched
    .select(
        "id",
        "subscription_id",
        "subscription_id_2",
        "status",
        "subscription_monthly_fee_2",
        "subscription_monthly_fee",
    )
    .unpivot(
        ids=["id", "subscription_id", "subscription_id_2", "status"],
        values=["subscription_monthly_fee_2", "subscription_monthly_fee"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
)
