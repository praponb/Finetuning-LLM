from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.started_at").desc(), F.col("c.id").desc())
w_running = Window.partitionBy("country").orderBy("age", "id")
w3 = Window.partitionBy("segment").orderBy("age", "id")
w_grp = Window.partitionBy("country").orderBy("prev_credit_limit", "id").rowsBetween(-5, Window.currentRow)
w_recent = Window.partitionBy("device", "country").orderBy(F.col("age").desc(), "id")
w6 = Window.partitionBy("country").orderBy("row_seq", "id").rowsBetween(-5, Window.currentRow)
w7 = Window.orderBy("age", "id").rowsBetween(-5, -1)
w8 = Window.orderBy(F.col("row_seq").desc(), "id")

with_latest = (
    df_customers
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.started_at") <= F.col("s.signup_date")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.started_at").alias("session_started_at"), F.col("c.device"))
)

with_windows = (
    with_latest
    .select(
        "id",
        "email",
        "country",
        "is_active",
        "age",
        "device",
        F.lead("age").over(w_running).alias("next_age"),
        F.lead("credit_limit").over(w_running).alias("next_credit_limit"),
        F.lag("credit_limit").over(w_running).alias("prev_credit_limit"),
        F.row_number().over(w_running).alias("event_seq"),
        F.row_number().over(w3).alias("row_seq"),
    )
)

metrics = (
    with_windows
    .filter(F.col("event_seq") >= 4)
    .withColumn("event_seq_2", F.row_number().over(w_running))
    .withColumn("next_age_avg_5", F.avg("next_age").over(w_grp))
    .withColumn("age_bucket", F.ntile(5).over(w_recent))
    .withColumn("next_credit_limit_avg_5", F.avg("next_credit_limit").over(w6))
)

metrics_customers = (
    metrics
    .select(
        "id",
        "country",
        "is_active",
        "device",
        "next_credit_limit",
        "prev_credit_limit",
        "event_seq",
        "row_seq",
        "event_seq_2",
        "age_bucket",
        "next_credit_limit_avg_5",
        F.avg("prev_credit_limit").over(w7).alias("prev_credit_limit_avg_5_prior"),
        F.ntile(4).over(w8).alias("row_seq_bucket"),
    )
)

result = (
    metrics_customers
    .rollup(
        F.when(F.col("prev_credit_limit") >= 16000, "LARGE").otherwise("SMALL").alias("prev_credit_limit_tier"),
        F.when(F.col("next_credit_limit") >= 13200, "PREMIUM").otherwise("STANDARD").alias("next_credit_limit_tier"),
        F.when(F.col("age_bucket") >= 8, "PREMIUM").otherwise("STANDARD").alias("age_bucket_tier"),
    )
    .agg(
        F.percentile_approx("prev_credit_limit", [0.25, 0.5, 0.75]).alias("prev_credit_limit_quartiles"),
        F.sum(F.when(F.col("country") == "TH", F.col("event_seq")).otherwise(0)).alias("th_event_seq"),
        F.round(F.sum("next_credit_limit") / F.nullif(F.count(F.when(F.col("country") == "US", 1)), F.lit(0)), 2).alias("next_credit_limit_per_us"),
        F.round(F.sum("age_bucket") / F.nullif(F.count(F.when(F.col("country") == "TH", 1)), F.lit(0)), 2).alias("age_bucket_per_th"),
        F.count_if(F.col("country") == "SG").alias("sg_events"),
        F.countDistinct("device").alias("distinct_devices"),
    )
    .orderBy(F.col("th_event_seq").desc())
)
