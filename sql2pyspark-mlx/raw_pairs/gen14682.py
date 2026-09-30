from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.started_at").desc(), F.col("c.id").desc())

valid = (
    df_customers
    .filter((F.col("signup_date") >= F.to_date(F.lit("2023-03-17"))) & F.col("city").isNotNull())
    .withColumn("signup_date_month_end", F.last_day("signup_date"))
)

with_last = (
    valid
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.started_at").alias("session_started_at"),
        F.col("c.browser").alias("session_browser"),
    )
)

valid_customers = (
    with_last
    .filter((F.col("signup_date") > F.to_date(F.lit("2023-05-22"))) & F.col("city").isNotNull())
    .select(
        "id",
        "name",
        "email",
        "country",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        "session_started_at",
        "session_browser",
        F.coalesce("name", F.lit("N/A")).alias("name_filled"),
        F.date_sub("signup_date_month_end", 72).alias("signup_date_month_end_minus_72d"),
        F.coalesce("session_browser", F.lit("N/A")).alias("session_browser_filled"),
    )
)

eligible = (
    valid_customers
    .alias("s")
    .join(df_subscriptions.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.start_date") >= F.col("s.session_started_at")), "left_anti")
)

result = (
    eligible
    .groupBy(
        F.date_trunc("QUARTER", "signup_date").alias("quarter_start"),
        F.date_trunc("YEAR", "session_started_at").alias("year_start"),
    )
    .agg(
        F.corr("credit_limit", "age").alias("corr_credit_limit_age"),
        F.bit_and("age").alias("age_bit_and"),
        F.size(F.collect_set("country")).alias("num_countries"),
        F.percentile_approx("credit_limit", 0.5).alias("credit_limit_p50"),
    )
    .filter(F.col("age_bit_and") > 1)
)
