from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.monthly_fee").desc(), F.col("c.id").desc())

base = (
    df_customers
    .filter(F.col("city").isNotNull())
    .withColumn("name_country", F.concat("name", F.lit(" "), "country"))
    .withColumn("resolved_segment", F.coalesce("segment", F.lit("OTHER")))
)

kept = (
    base
    .alias("s")
    .join(df_accounts.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.opened_date") < F.col("s.signup_date")), "left_anti")
)

cleaned = (
    kept
    .filter(F.col("city").isNotNull() & (F.col("credit_limit") < 9900))
    .withColumn("country_email", F.concat("country", F.lit(" - "), "email"))
    .withColumn("country_prefix", F.substring("country", 1, 4))
    .withColumn("segment_filled", F.coalesce("segment", F.lit("NONE")))
)

with_latest = (
    cleaned
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.monthly_fee").alias("subscription_monthly_fee"),
        F.col("c.plan").alias("subscription_plan"),
    )
)

totals = (
    with_latest
    .withColumn("credit_limit_tier", F.when(F.col("credit_limit") >= 12300, "LARGE").otherwise("SMALL"))
    .withColumn("age_tier", F.when(F.col("age") >= 64, "LARGE").otherwise("SMALL"))
    .rollup("credit_limit_tier", "age_tier", "country")
    .agg(
        F.corr("credit_limit", "age").alias("corr_credit_limit_age"),
        F.avg("subscription_monthly_fee").alias("avg_subscription_monthly_fee"),
        F.round(F.sum("age") / F.nullif(F.count(F.when(F.col("city") == "Bangkok", 1)), F.lit(0)), 2).alias("age_per_bangkok"),
        F.count(F.when(F.col("country") == "JP", 1)).alias("jp_count"),
        F.sum(F.when(F.col("subscription_plan") == "standard", F.col("age"))).alias("standard_total_age"),
        F.max(F.when(F.col("segment") == "SMB", F.col("credit_limit"))).alias("smb_max_credit_limit"),
        F.grouping("credit_limit_tier").alias("gid"),
    )
)

result = (
    totals
    .select(
        "credit_limit_tier",
        "age_tier",
        "corr_credit_limit_age",
        "avg_subscription_monthly_fee",
        "standard_total_age",
        "smb_max_credit_limit",
        "gid",
    )
    .orderBy(F.col("gid").desc_nulls_last(), "corr_credit_limit_age")
)
