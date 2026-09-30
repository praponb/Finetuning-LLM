from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("signup_date"), "city")
w2 = Window.partitionBy("segment", "city")
w3 = Window.partitionBy("city").orderBy("signup_date", "id")
w4 = Window.partitionBy(F.year("signup_date"), "city").orderBy(F.col("credit_limit").desc(), "id")
w_running = Window.partitionBy("city", "segment").orderBy("signup_date_month_sum_age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.balance").desc(), F.col("c.id").desc())

with_windows = (
    df_customers
    .withColumn("signup_date_month_sum_age", F.sum("age").over(w))
    .withColumn("credit_limit_share", F.round((F.col("credit_limit") / F.sum("credit_limit").over(w2)), 4))
)

with_windows_customers = (
    with_windows
    .filter(F.col("signup_date_month_sum_age") == 195)
    .select(
        "id",
        "country",
        "city",
        "segment",
        "signup_date_month_sum_age",
        F.row_number().over(w3).alias("row_seq"),
        F.row_number().over(w4).alias("event_seq"),
    )
)

not_flagged = (
    with_windows_customers
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.device") == "desktop"), "left_anti")
)

metrics = (
    not_flagged
    .withColumn("running_avg_event_seq", F.avg("event_seq").over(w_running))
    .withColumn("running_count_event_seq", F.count("event_seq").over(w_running))
    .withColumn("running_min_event_seq", F.min("event_seq").over(w_running))
    .withColumn("event_seq_share", F.round((F.col("event_seq") / F.sum("event_seq").over(w2)), 4))
)

with_latest = (
    metrics
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.account_type"), F.col("c.balance"))
)

valid = (
    with_latest
    .filter(F.col("signup_date_month_sum_age") >= 117)
    .select(
        "id",
        "country",
        "city",
        "signup_date_month_sum_age",
        "row_seq",
        "running_avg_event_seq",
        "running_count_event_seq",
        "running_min_event_seq",
        "event_seq_share",
        F.round((F.col("balance") * 100), 2).alias("balance_adjusted"),
    )
)

result = (
    valid
    .cube("country")
    .agg(
        F.round(F.sum("running_avg_event_seq") / F.nullif(F.count(F.when(F.col("country") == "US", 1)), F.lit(0)), 2).alias("running_avg_event_seq_per_us"),
        F.percentile_approx("event_seq_share", [0.25, 0.5, 0.75]).alias("event_seq_share_quartiles"),
        F.percentile_approx("signup_date_month_sum_age", [0.25, 0.5, 0.75]).alias("signup_date_month_sum_age_quartiles"),
        F.grouping_id().alias("grouping_id"),
    )
)
