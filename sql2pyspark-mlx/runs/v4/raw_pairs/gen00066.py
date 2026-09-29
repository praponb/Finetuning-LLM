from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id")
w2 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("country")
w4 = Window.partitionBy("browser").orderBy("is_new_burst", "id")
w5 = Window.orderBy("session_id", "id").rowsBetween(-5, -1)
w6 = Window.partitionBy("device", "country").orderBy(F.col("session_id").desc(), "id")
w7 = Window.partitionBy("browser")
w8 = Window.partitionBy("country").orderBy("monthly_fee", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("plan")
w10 = Window.partitionBy("browser").orderBy(F.col("started_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w11 = Window.partitionBy("customer_id").orderBy(F.col("session_id").desc(), "id", "subscription_id")

cleaned = (
    df_sessions
    .filter(F.col("country").isNotNull() & (F.col("started_at") < "2024-06-01"))
    .withColumn("browser_prefix", F.substring("browser", 1, 4))
    .withColumn("country_clean", F.coalesce("country", F.lit("UNKNOWN")))
)

gaps = (
    cleaned
    .withColumn("is_new_burst", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessions_sessions = gaps.withColumn("session_id", F.sum("is_new_burst").over(w2))

joined = (
    sessions_sessions
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id") == F.col("c.id"), "left")
    .select("e.*", F.col("c.email"), F.col("c.signup_date"))
)

with_windows = (
    joined
    .select(
        "*",
        F.max("session_id").over(w3).alias("country_max_session_id"),
        F.lead("session_id", 1).over(w4).alias("next_session_id"),
        F.avg("is_new_burst").over(w5).alias("is_new_burst_avg_5_prior"),
        F.row_number().over(w6).alias("seq_no"),
    )
)

matched = (
    with_windows
    .alias("src")
    .join(df_subscriptions.alias("sub"), (F.col("src.customer_id") == F.col("sub.customer_id")) & F.col("src.signup_date").between(F.col("sub.start_date"), F.col("sub.end_date")), "left")
    .select(
        "src.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

result = (
    matched
    .select(
        "*",
        F.stddev_samp("country_max_session_id").over(w7).alias("browser_stddev_samp_country_max_session_id"),
        F.last("seq_no", ignorenulls=True).over(w8).alias("last_known_seq_no"),
        F.min("is_new_burst_avg_5_prior").over(w9).alias("plan_min_is_new_burst_avg_5_prior"),
        F.count("country_max_session_id").over(w10).alias("country_max_session_id_count_30d"),
        F.ntile(4).over(w11).alias("session_id_bucket"),
    )
)
