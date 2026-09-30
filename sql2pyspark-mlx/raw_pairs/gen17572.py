from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("city").orderBy(F.col("running_sum_age").asc_nulls_first(), "id")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.opened_date").desc(), F.col("c.id").desc())
w4 = Window.partitionBy("city").orderBy(F.col("chiang_mai_events").desc())

filtered = (
    df_customers
    .filter(F.col("segment").isNotNull() & (~F.col("segment").eqNullSafe(F.lit("Consumer"))))
    .select(
        "*",
        F.round("credit_limit", 2).alias("credit_limit_rounded"),
        F.when(F.col("is_active"), "Yes").otherwise("No").alias("is_active_label"),
        F.coalesce("city", F.lit("UNKNOWN")).alias("city_filled"),
    )
)

scored = (
    filtered
    .select(
        "id",
        "name",
        "email",
        "city",
        "signup_date",
        "credit_limit",
        "is_active_label",
        "city_filled",
        F.count("age").over(w).alias("running_count_age"),
        F.sum("age").over(w).alias("running_sum_age"),
        F.min("age").over(w).alias("running_min_age"),
    )
)

eligible = (
    scored
    .alias("s")
    .join(df_accounts.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.opened_date") < F.col("s.signup_date")), "left_semi")
)

latest = (
    eligible
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

filtered_customers = (
    latest
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "name",
        "city",
        "signup_date",
        "credit_limit",
        "is_active_label",
        "city_filled",
        "running_sum_age",
        "running_min_age",
        F.least("running_count_age", "running_sum_age").alias("least_running_count_age_running_sum_age"),
        F.coalesce("city", F.lit("DIRECT")).alias("resolved_city"),
    )
)

with_last = (
    filtered_customers
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.opened_date").alias("account_opened_date"))
)

result = (
    with_last
    .groupBy("city", F.date_trunc("YEAR", "signup_date").alias("year_start"))
    .agg(
        F.count_if(F.col("city") == "Chiang Mai").alias("chiang_mai_events"),
        F.percentile_approx("credit_limit", 0.5).alias("credit_limit_p50"),
    )
    .filter(F.col("credit_limit_p50") >= 0)
    .withColumn("chiang_mai_events_rank", F.rank().over(w4))
    .orderBy("city")
)
