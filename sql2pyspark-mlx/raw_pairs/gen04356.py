from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("signup_date", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.started_at").desc(), F.col("c.id").desc())
w_recent = Window.partitionBy("session_device").orderBy("last3_count_age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("session_country")
w5 = Window.partitionBy("session_country").orderBy(F.col("running_max_last3_max_age").desc(), "id")

not_flagged = (
    df_customers
    .alias("s")
    .join(df_subscriptions.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.end_date") <= F.col("s.signup_date")), "left_anti")
)

enriched = (
    not_flagged
    .filter(F.col("credit_limit") > 15600)
    .select(
        "id",
        "city",
        "signup_date",
        "is_active",
        "credit_limit",
        F.count("age").over(w).alias("last3_count_age"),
        F.max("age").over(w).alias("last3_max_age"),
        F.avg("age").over(w).alias("last3_avg_age"),
    )
)

with_latest = (
    enriched
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.started_at") <= F.col("s.signup_date")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.device").alias("session_device"),
        F.col("c.country").alias("session_country"),
    )
)

metrics = (
    with_latest
    .withColumn("running_min_last3_max_age", F.min("last3_max_age").over(w_recent))
    .withColumn("running_sum_last3_max_age", F.sum("last3_max_age").over(w_recent))
    .withColumn("running_max_last3_max_age", F.max("last3_max_age").over(w_recent))
    .withColumn("session_country_avg_last3_count_age", F.avg("last3_count_age").over(w_ordered))
)

ranked = (
    metrics
    .withColumn("dedup_rank", F.row_number().over(w5))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    ranked
    .select(
        "id",
        "city",
        "signup_date",
        "last3_count_age",
        "last3_max_age",
        "session_device",
        "running_min_last3_max_age",
        "session_country_avg_last3_count_age",
        "dedup_rank",
    )
)
