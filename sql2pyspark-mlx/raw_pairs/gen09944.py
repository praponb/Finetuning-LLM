from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("age_rounded").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.started_at").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("session_device").orderBy(F.col("age_rounded").asc_nulls_first())
w4 = Window.partitionBy("city").orderBy("signup_date", "id").rowsBetween(-5, Window.currentRow)
w5 = Window.partitionBy("city").orderBy(F.col("started_at").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w6 = Window.partitionBy("city")
w7 = Window.partitionBy("session_device", "segment").orderBy(F.col("age_rounded").desc_nulls_last(), "id")
w_recent = Window.orderBy("age_rounded", "id").rowsBetween(-5, -1)
w9 = Window.partitionBy("segment", "country").orderBy(F.col("age").desc())
w10 = Window.partitionBy("country").orderBy("city_max_credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("country").orderBy(F.col("started_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)

filtered = (
    df_customers
    .filter((F.col("signup_date") >= F.lit("2023-09-10").cast("date")) & F.col("is_active"))
    .select(
        "*",
        F.round("age", 0).alias("age_rounded"),
        F.coalesce("segment", F.lit("Unknown")).alias("segment_filled"),
        F.coalesce("segment", F.lit("OTHER")).alias("segment_filled_2"),
    )
)

deduped = (
    filtered
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

latest_match = (
    deduped
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.started_at") <= F.col("s.signup_date")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.device").alias("session_device"), F.col("c.started_at"))
)

metrics = (
    latest_match
    .select(
        "*",
        F.rank().over(w3).alias("age_rounded_rank"),
        F.avg("age_rounded").over(w4).alias("age_rounded_avg_5"),
        F.max("credit_limit").over(w5).alias("credit_limit_max_90d"),
        F.max("credit_limit").over(w6).alias("city_max_credit_limit"),
    )
)

deduped_customers = (
    metrics
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
)

enriched = (
    deduped_customers
    .select(
        "id",
        "name",
        "segment",
        "signup_date",
        "credit_limit",
        "age_rounded",
        "session_device",
        "started_at",
        "age_rounded_avg_5",
        "credit_limit_max_90d",
        "rn",
        F.avg("credit_limit_max_90d").over(w_recent).alias("credit_limit_max_90d_avg_5_prior"),
        F.percent_rank().over(w9).alias("age_percent_rank"),
        F.sum("age_rounded_avg_5").over(w10).alias("running_age_rounded_avg_5"),
        F.count("age_rounded_avg_5").over(w_running).alias("age_rounded_avg_5_count_7d"),
    )
)

result = (
    enriched
    .select(
        "id",
        "name",
        "segment",
        "signup_date",
        "credit_limit",
        "age_rounded",
        "session_device",
        "started_at",
        "age_rounded_avg_5",
    )
    .orderBy(F.col("age_rounded_avg_5").desc_nulls_last())
)
