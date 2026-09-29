from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("signup_date").asc_nulls_last(), "id")
w2 = Window.partitionBy("city").orderBy(F.col("age").desc(), "id")
w3 = Window.partitionBy("country").orderBy(F.col("avg_age_times_latest_rank").desc())

cleaned = (
    df_customers
    .filter((F.col("signup_date") > F.lit("2023-03-06").cast("date")) & F.col("name").like("Mi%"))
    .withColumn("email_user", F.split(F.lower("email"), "@").getItem(0))
    .withColumn("segment_clean", F.coalesce("segment", F.lit("DIRECT")))
)

deduped = (
    cleaned
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

deduped_customers = (
    deduped
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

filtered = (
    deduped_customers
    .filter(F.col("segment").isNotNull())
    .select(
        "*",
        F.round((F.col("latest_rank") / F.col("age")), 2).alias("latest_rank_per_age"),
        (F.col("age") * F.col("latest_rank")).alias("age_times_latest_rank"),
    )
)

kept = (
    filtered
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.started_at") <= F.col("s.signup_date")), "left_anti")
)

cleaned_customers = (
    kept
    .filter(F.col("city").isNotNull() & (F.col("age_times_latest_rank") >= 64))
    .select(
        "id",
        "name",
        "email",
        "country",
        "segment",
        "credit_limit",
        "email_user",
        "segment_clean",
        "age_times_latest_rank",
        (F.col("age_times_latest_rank") + F.col("credit_limit")).alias("age_times_latest_rank_plus_credit_limit"),
    )
)

not_flagged = (
    cleaned_customers
    .alias("s")
    .join(df_sessions.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.country").isin(["SG", "TH", "UK"])), "left_anti")
)

result = (
    not_flagged
    .groupBy("country", "segment")
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"),
        F.percentile_approx("age_times_latest_rank", 0.95).alias("age_times_latest_rank_p95"),
        F.round(F.avg("age_times_latest_rank"), 2).alias("avg_age_times_latest_rank"),
    )
    .withColumn("avg_age_times_latest_rank_rank", F.rank().over(w3))
    .orderBy(F.col("avg_credit_limit").desc_nulls_first())
)
