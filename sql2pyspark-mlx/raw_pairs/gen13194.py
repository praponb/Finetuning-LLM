from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("age").desc(), "id")
w2 = Window.partitionBy("segment", "city").orderBy(F.col("country_length").desc(), "id")
w3 = Window.orderBy("age", "id")
w4 = Window.orderBy(F.col("age").desc(), "id")
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.monthly_fee").desc(), F.col("c.id").desc())

base = (
    df_customers
    .filter(F.col("signup_date") >= F.to_date(F.lit("2023-05-22")))
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "segment",
        "age",
        F.length("country").alias("country_length"),
    )
)

ranked = (
    base
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

ranked_customers = (
    ranked
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
    .select("id", "name", "email", "age")
)

not_flagged = (
    ranked_customers
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (~F.col("c.country").isin(["SG", "TH"])), "left_anti")
)

with_windows = (
    not_flagged
    .select(
        "id",
        "name",
        F.lead("age", 1).over(w3).alias("next_age"),
        F.row_number().over(w4).alias("event_seq"),
    )
)

with_latest = (
    with_windows
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.plan").alias("subscription_plan"),
        F.col("c.monthly_fee").alias("subscription_monthly_fee"),
    )
)

result = (
    with_latest
    .groupBy(
        F.when(F.col("subscription_monthly_fee") >= 193, "HIGH").otherwise("LOW").alias("subscription_monthly_fee_tier"),
    )
    .agg(
        F.avg("subscription_monthly_fee").alias("avg_subscription_monthly_fee"),
        F.countDistinct("id").alias("distinct_ids"),
        F.max(F.when(F.col("subscription_plan") == "enterprise", F.col("subscription_monthly_fee"))).alias("enterprise_max_subscription_monthly_fee"),
        F.count_if(F.col("subscription_plan") == "premium").alias("premium_events"),
        F.sum(F.when(F.col("subscription_plan") == "enterprise", F.col("next_age")).otherwise(0)).alias("enterprise_next_age"),
        F.count(F.when(F.col("subscription_plan") == "enterprise", 1)).alias("enterprise_rows"),
    )
)
