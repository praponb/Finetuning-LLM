from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.end_date").desc(), F.col("c.id").desc())

valid = (
    df_customers
    .filter((F.col("signup_date") > F.lit("2023-04-05").cast("date")) & F.col("city").isNotNull())
    .select(
        "id",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        F.round((F.col("credit_limit") / F.col("age")), 2).alias("credit_limit_per_age"),
        F.coalesce("segment", F.lit("UNKNOWN")).alias("segment_filled"),
    )
)

cleaned = (
    valid
    .filter(~F.col("segment").eqNullSafe(F.lit("SMB")))
    .withColumn("is_active_label", F.when(F.col("is_active"), "Yes").otherwise("No"))
    .withColumn("city_clean", F.coalesce("city", F.lit("UNKNOWN")))
)

not_flagged = (
    cleaned
    .alias("s")
    .join(df_subscriptions.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.monthly_fee").between(60, 120)), "left_semi")
)

filtered = (
    not_flagged
    .filter((F.col("signup_date") >= F.to_date(F.lit("2023-06-26"))) & (F.col("city").eqNullSafe(F.lit("Tokyo"))))
    .withColumn("segment_email_key", F.concat_ws("-", "segment", "email"))
    .withColumn("email_upper", F.upper("email"))
)

latest_match = (
    filtered
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.end_date") <= F.col("s.signup_date")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.email",
        "s.city",
        "s.age",
        "s.credit_limit_per_age",
        "s.city_clean",
        "s.segment_email_key",
        "s.email_upper",
        F.col("c.plan").alias("subscription_plan"),
    )
)

result = (
    latest_match
    .rollup("subscription_plan", "city")
    .agg(
        F.sum(F.when(F.col("subscription_plan") == "standard", F.col("credit_limit_per_age"))).alias("standard_total_credit_limit_per_age"),
        F.round(F.avg("age"), 2).alias("avg_age"),
        F.size(F.collect_set("subscription_plan")).alias("num_subscription_plans"),
        F.count(F.when(F.col("city") == "Singapore", 1)).alias("singapore_rows"),
        F.bit_or("age").alias("age_bit_or"),
        F.grouping("subscription_plan").alias("grouping_level"),
    )
    .orderBy(F.col("singapore_rows").asc_nulls_last())
)
