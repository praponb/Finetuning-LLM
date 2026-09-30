from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment", "country").orderBy("credit_limit")
w_running = Window.partitionBy("segment").orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_customers
    .filter(F.col("signup_date") >= F.to_date(F.lit("2023-08-31")))
    .select(
        "*",
        F.when(F.col("is_active"), "Yes").otherwise("No").alias("is_active_label"),
        F.coalesce("email", F.lit("unassigned")).alias("email_filled"),
        F.coalesce("segment", F.lit("N/A")).alias("segment_filled"),
    )
)

deduped = (
    valid
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)

with_windows = (
    deduped
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "signup_date",
        "is_active",
        "age",
        "segment_filled",
        "rn",
        F.sum("age").over(w_running).alias("running_sum_age"),
        F.max("age").over(w_running).alias("running_max_age"),
        F.min("age").over(w_running).alias("running_min_age"),
        F.count("age").over(w_running).alias("running_count_age"),
    )
)

filtered = (
    with_windows
    .filter(F.col("signup_date") >= F.lit("2023-08-07").cast("date"))
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "signup_date",
        "is_active",
        "age",
        "rn",
        "running_sum_age",
        "running_max_age",
        "running_count_age",
        (F.col("running_count_age") - F.col("running_sum_age")).alias("running_count_age_minus_running_sum_age"),
        (F.col("running_max_age") * F.col("rn")).alias("running_max_age_times_rn"),
    )
)

not_flagged = (
    filtered
    .alias("s")
    .join(df_accounts.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.opened_date") <= F.col("s.signup_date")), "left_semi")
)

result = (
    not_flagged
    .select(
        "id",
        "email",
        "country",
        "age",
        "rn",
        "running_max_age",
        "running_count_age_minus_running_sum_age",
        "running_max_age_times_rn",
    )
    .orderBy(F.col("running_count_age_minus_running_sum_age").asc_nulls_last())
)
