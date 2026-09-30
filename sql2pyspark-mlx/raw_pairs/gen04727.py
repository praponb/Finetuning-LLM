from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("browser").orderBy("started_at", "id")
w3 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("segment").orderBy("started_at", "id")
w5 = Window.partitionBy("segment").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy(F.to_date("started_at"), "customer_id").orderBy("started_at", "id").rowsBetween(-3, Window.currentRow)
w7 = Window.partitionBy("customer_id", "segment").orderBy(F.col("session_no").desc())
w8 = Window.partitionBy("country")
w9 = Window.partitionBy(F.month("started_at")).orderBy("session_no", "id").rowsBetween(-6, -1)
w10 = Window.partitionBy("customer_id", "segment").orderBy("session_no", "id")

latest_match = (
    df_sessions
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.event_type"))
)

gaps = (
    latest_match
    .withColumn("is_new_session", F.when(F.col("started_at") - F.lag("started_at").over(w2) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

sessionized = (
    gaps
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        "event_type",
        F.sum("is_new_session").over(w3).alias("session_id"),
    )
)

enriched = (
    sessionized
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "inner")
    .filter(~F.col("d.is_active"))
    .select(
        "a.id",
        "a.customer_id",
        "a.started_at",
        "a.country",
        F.col("d.segment"),
        F.col("d.signup_date"),
    )
)

gaps_2 = (
    enriched
    .withColumn("is_new_burst", F.when(F.lag("started_at").over(w4).isNull() | (F.col("started_at") > F.lag("started_at").over(w4) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessions_sessions = (
    gaps_2
    .select(
        "id",
        "customer_id",
        "started_at",
        "country",
        "segment",
        "signup_date",
        F.sum("is_new_burst").over(w5).alias("session_no"),
    )
)

scored = (
    sessions_sessions
    .select(
        "id",
        "customer_id",
        "started_at",
        "country",
        "segment",
        "signup_date",
        F.avg("session_no").over(w6).alias("session_no_avg_3"),
        F.dense_rank().over(w7).alias("session_no_dense_rank"),
        F.sum("session_no").over(w8).alias("country_sum_session_no"),
        F.avg("session_no").over(w9).alias("session_no_avg_6_prior"),
        F.ntile(5).over(w10).alias("session_no_bucket"),
    )
)

result = (
    scored
    .select(
        "id",
        "started_at",
        "country",
        "segment",
        "signup_date",
        "session_no_dense_rank",
        "country_sum_session_no",
        "session_no_bucket",
        F.when(F.col("country").rlike("^(US|DE)$"), "EXTENDED").when(F.col("country").rlike("^(TH|SG)$"), "GROUP_B").otherwise("OTHER_ZONE").alias("country_zone"),
    )
    .orderBy(F.col("session_no_dense_rank").asc_nulls_first(), "signup_date")
)
