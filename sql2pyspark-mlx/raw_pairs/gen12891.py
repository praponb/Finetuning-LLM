from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("credit_limit", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("country").orderBy("signup_date", "id")
w3 = Window.partitionBy("country").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("city").orderBy("prev_age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("country").orderBy("age_avg_6", "id")

base = (
    df_customers
    .filter(F.upper(F.trim("email")) == "GRACE.KIM@GMAIL.COM")
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        F.trim("country").alias("clean_country"),
    )
)

with_windows = (
    base
    .filter(F.col("name").like("b%"))
    .select(
        "*",
        F.avg("age").over(w).alias("age_avg_6"),
        F.lag("age", 2, 0).over(w2).alias("prev_age"),
    )
)

kept = (
    with_windows
    .alias("s")
    .join(df_sessions.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.started_at") < F.col("s.signup_date")), "left_anti")
)

enriched = (
    kept
    .select(
        "id",
        "country",
        "city",
        "credit_limit",
        "age",
        "prev_age",
        F.last("city", ignorenulls=True).over(w3).alias("last_known_city"),
        F.sum("credit_limit").over(w4).alias("cumulative_credit_limit"),
        F.ntile(4).over(w5).alias("age_avg_6_bucket"),
    )
)

summary = (
    enriched
    .groupBy(
        F.when(F.col("age_avg_6_bucket") >= 8, "PREMIUM").otherwise("STANDARD").alias("age_avg_6_bucket_tier"),
        F.when(F.col("cumulative_credit_limit") >= 84977, "HIGH").otherwise("LOW").alias("cumulative_credit_limit_tier"),
    )
    .agg(
        F.round(F.sum("age") / F.nullif(F.count(F.when(F.col("country") == "US", 1)), F.lit(0)), 2).alias("age_per_us"),
        F.sum(F.when(F.col("city") == "New York", F.col("cumulative_credit_limit")).otherwise(0)).alias("new_york_cumulative_credit_limit"),
        F.round(F.sum("age") / F.nullif(F.count(F.when(F.col("country") == "UK", 1)), F.lit(0)), 2).alias("age_per_uk"),
        F.stddev_samp("credit_limit").alias("credit_limit_stddev"),
        F.count("*").alias("event_count"),
        F.max(F.when(F.col("city") == "Berlin", F.col("credit_limit"))).alias("berlin_max_credit_limit"),
    )
)

result = (
    summary
    .select(
        "age_avg_6_bucket_tier",
        "cumulative_credit_limit_tier",
        "age_per_us",
        "new_york_cumulative_credit_limit",
        "age_per_uk",
        "credit_limit_stddev",
        "event_count",
        "berlin_max_credit_limit",
    )
)
