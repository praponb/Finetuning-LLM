from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("segment")
w2 = Window.partitionBy(F.date_trunc("WEEK", "signup_date"), "segment")
w3 = Window.partitionBy("city")

valid = (
    df_customers
    .filter(F.col("age") > 48)
    .select(
        "id",
        "name",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        F.regexp_replace("email", " ", "_").alias("email_slug"),
    )
)

remaining_keys = (
    valid
    .select("id", "segment")
    .exceptAll(
        df_customers
        .filter(F.col("credit_limit") <= 13700)
        .select("id", "segment")
    )
)

verified = (
    valid
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.segment") == F.col("k.segment")), "inner")
    .select("s.*")
)

not_flagged = (
    verified
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.order_date") <= F.col("s.signup_date")), "left_semi")
)

with_windows = (
    not_flagged
    .select(
        "*",
        F.stddev_samp("credit_limit").over(w_running).alias("segment_stddev_samp_credit_limit"),
        F.sum("credit_limit").over(w_running).alias("segment_sum_credit_limit"),
        F.stddev_samp("credit_limit").over(w_running).alias("segment_stddev_samp_credit_limit_2"),
    )
)

metrics = (
    with_windows
    .select(
        "id",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        F.stddev_samp("segment_stddev_samp_credit_limit_2").over(w2).alias("signup_date_period_stddev_samp_segment_stddev_samp_credit_limit_2"),
        F.round((F.col("segment_stddev_samp_credit_limit_2") / F.sum("segment_stddev_samp_credit_limit_2").over(w3)), 4).alias("segment_stddev_samp_credit_limit_2_share"),
    )
)

result = (
    metrics
    .select(
        "id",
        "is_active",
        "credit_limit",
        "signup_date_period_stddev_samp_segment_stddev_samp_credit_limit_2",
        "segment_stddev_samp_credit_limit_2_share",
    )
    .orderBy(F.col("credit_limit").desc())
)
