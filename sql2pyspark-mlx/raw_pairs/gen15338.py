from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("age").asc_nulls_first())
w2 = Window.partitionBy("segment").orderBy(F.col("age").desc())
w_running = Window.partitionBy("segment").orderBy("credit_limit", "id").rowsBetween(-6, Window.currentRow)
w_recent = Window.partitionBy("segment").orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_customers
    .filter((F.col("signup_date") >= F.to_date(F.lit("2023-05-04"))) & F.upper("email").like("%LIC%"))
    .select("*", F.upper(F.trim("name")).alias("name_normalized"))
)

latest = (
    filtered
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "name",
        "email",
        "country",
        "segment",
        "is_active",
        "credit_limit",
        "age",
    )
)

cleaned = (
    latest
    .filter(F.col("segment").isNotNull() & (~F.col("segment").eqNullSafe(F.lit("Corporate"))))
    .select(
        "id",
        "name",
        "segment",
        "is_active",
        "credit_limit",
        "age",
        F.substring("email", 1, 4).alias("email_prefix"),
    )
)

latest_customers = (
    cleaned
    .withColumn("latest_rank", F.rank().over(w2))
    .filter(F.col("latest_rank") == 1)
)

with_windows = (
    latest_customers
    .filter(F.col("latest_rank") != 1)
    .select(
        "*",
        F.avg("credit_limit").over(w_running).alias("last7_avg_credit_limit"),
        F.sum("credit_limit").over(w_running).alias("last7_sum_credit_limit"),
        F.min("credit_limit").over(w_running).alias("last7_min_credit_limit"),
    )
)

scored = (
    with_windows
    .select(
        "id",
        "credit_limit",
        "age",
        "email_prefix",
        "latest_rank",
        "last7_avg_credit_limit",
        "last7_sum_credit_limit",
        F.sum("age").over(w_recent).alias("running_sum_age"),
        F.min("age").over(w_recent).alias("running_min_age"),
        F.avg("age").over(w_recent).alias("running_avg_age"),
        F.max("age").over(w_recent).alias("running_max_age"),
    )
)

stats = (
    scored
    .groupBy(
        F.when(F.col("last7_avg_credit_limit") >= 13300, "LARGE").otherwise("SMALL").alias("last7_avg_credit_limit_tier"),
    )
    .agg(
        F.percentile_approx("running_max_age", [0.25, 0.5, 0.75]).alias("running_max_age_quartiles"),
        F.sum("last7_avg_credit_limit").alias("total_last7_avg_credit_limit"),
    )
)

result = stats.select("last7_avg_credit_limit_tier", "running_max_age_quartiles")
