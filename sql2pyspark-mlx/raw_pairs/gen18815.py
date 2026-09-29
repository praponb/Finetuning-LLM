from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.started_at").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("segment").orderBy("credit_limit", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy("session_device").orderBy(F.col("last4_min_credit_limit").desc(), "id")
w4 = Window.partitionBy("segment").orderBy("credit_limit", "id").rowsBetween(-2, -1)

cleaned = (
    df_customers
    .filter(F.col("signup_date") >= F.to_date(F.lit("2023-02-15")))
    .select(
        "id",
        "name",
        "country",
        "segment",
        "is_active",
        "credit_limit",
        F.round((F.col("age") * 1.15), 2).alias("age_scaled"),
        F.concat_ws("/", "country", "email").alias("country_email_key"),
    )
)

with_last = (
    cleaned
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.segment",
        "s.is_active",
        "s.credit_limit",
        "s.country_email_key",
        F.col("c.device").alias("session_device"),
    )
)

metrics = (
    with_last
    .withColumn("last4_min_credit_limit", F.min("credit_limit").over(w2))
    .withColumn("last4_sum_credit_limit", F.sum("credit_limit").over(w2))
    .withColumn("last4_avg_credit_limit", F.avg("credit_limit").over(w2))
)

metrics_customers = (
    metrics
    .select(
        "id",
        "segment",
        "credit_limit",
        "session_device",
        "last4_min_credit_limit",
        "last4_sum_credit_limit",
        "last4_avg_credit_limit",
        F.ntile(3).over(w3).alias("last4_min_credit_limit_bucket"),
        F.avg("last4_avg_credit_limit").over(w4).alias("last4_avg_credit_limit_avg_2_prior"),
    )
)

daily = (
    metrics_customers
    .groupBy("segment")
    .agg(
        F.round(F.avg("last4_sum_credit_limit"), 2).alias("avg_last4_sum_credit_limit"),
        F.sort_array(F.collect_set("session_device")).alias("session_device_list"),
        F.percentile_approx("last4_min_credit_limit", 0.9).alias("last4_min_credit_limit_p90"),
        F.round(F.avg("last4_avg_credit_limit"), 2).alias("avg_last4_avg_credit_limit"),
        F.min("credit_limit").alias("min_credit_limit"),
        F.count_if(F.col("segment") == "SMB").alias("smb_events"),
    )
)

result = (
    daily
    .select(
        "segment",
        "avg_last4_sum_credit_limit",
        "session_device_list",
        "avg_last4_avg_credit_limit",
        "min_credit_limit",
        "smb_events",
    )
    .orderBy(F.col("avg_last4_avg_credit_limit").desc())
)
