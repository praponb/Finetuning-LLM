from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.started_at").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("city").orderBy(F.col("credit_limit").desc())
w3 = Window.partitionBy("session_browser").orderBy("session_started_at", "id")
w4 = Window.partitionBy("session_browser").orderBy("session_started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_customers
    .filter(F.col("country") == "DE")
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "credit_limit",
        "age",
        F.substring("email", 1, 3).alias("email_prefix"),
    )
)

kept = (
    cleaned
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.store_id") == 34), "left_anti")
)

latest_match = (
    kept
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.country").alias("session_country"),
        F.col("c.browser").alias("session_browser"),
    )
)

latest = (
    latest_match
    .withColumn("latest_rank", F.rank().over(w2))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

with_last = (
    latest
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.country",
        "s.credit_limit",
        "s.age",
        "s.session_browser",
        F.col("c.started_at").alias("session_started_at"),
        F.col("c.country").alias("session_country_2"),
    )
)

kept_customers = (
    with_last
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.order_date") >= F.col("s.session_started_at")), "left_anti")
)

with_gaps = (
    kept_customers
    .withColumn("is_new_burst", F.when(F.col("session_started_at") - F.lag("session_started_at").over(w3) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

sessions_customers = with_gaps.withColumn("burst_id", F.sum("is_new_burst").over(w4))

stats = (
    sessions_customers
    .groupBy(
        F.when(F.col("age") >= 63, "PREMIUM").otherwise("STANDARD").alias("age_tier"),
        F.when(F.col("is_new_burst") >= 1, "PREMIUM").otherwise("STANDARD").alias("is_new_burst_tier"),
    )
    .agg(
        F.count_if(F.col("session_browser") == "Safari").alias("safari_events"),
        F.min("age").alias("min_age"),
        F.min("session_started_at").alias("started_at"),
        F.max("session_started_at").alias("last_seen"),
    )
)

result = (
    stats
    .select("age_tier", "is_new_burst_tier", "min_age", "started_at")
    .orderBy(F.col("age_tier").desc(), "min_age")
)
