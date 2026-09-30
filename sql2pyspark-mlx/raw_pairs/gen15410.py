from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "segment").orderBy(F.col("signup_date").asc_nulls_last(), "id")
w2 = Window.partitionBy("city")
w_hist = Window.partitionBy("segment").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("country")
w_recent = Window.orderBy(F.col("credit_limit").desc(), "id")
w6 = Window.partitionBy(F.date_trunc("YEAR", "signup_date"))
w7 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.started_at").desc(), F.col("c.id").desc())
w8 = Window.partitionBy("segment").orderBy("session_started_at", "id")
w9 = Window.partitionBy("segment").orderBy("session_started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy(F.year("session_started_at"), "session_device").orderBy("credit_limit", "id")
w11 = Window.partitionBy("session_device", "segment")

deduped = (
    df_customers
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

enriched = (
    deduped
    .withColumn("corr_age_credit_limit", F.corr("age", "credit_limit").over(w2))
    .withColumn("cumulative_age", F.sum("age").over(w_hist))
    .withColumn("age_share", F.round((F.col("age") / F.sum("age").over(w4)), 4))
    .withColumn("credit_limit_bucket", F.ntile(10).over(w_recent))
    .withColumn("signup_date_period_max_age", F.max("age").over(w6))
)

with_latest = (
    enriched
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.started_at") <= F.col("s.signup_date")), "left")
    .withColumn("rn_2", F.row_number().over(w7))
    .filter(F.col("rn_2") == 1)
    .select(
        "s.id",
        "s.country",
        "s.segment",
        "s.credit_limit",
        "s.age",
        "s.age_share",
        F.col("c.started_at").alias("session_started_at"),
        F.col("c.device").alias("session_device"),
    )
)

with_gaps = (
    with_latest
    .withColumn("is_new_burst", F.when(F.col("session_started_at") - F.lag("session_started_at").over(w8) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

islands = (
    with_gaps
    .select(
        "id",
        "segment",
        "credit_limit",
        "age",
        "session_started_at",
        "session_device",
        F.sum("is_new_burst").over(w9).alias("session_no"),
    )
)

metrics = (
    islands
    .select(
        "*",
        F.first("segment", ignorenulls=True).over(w9).alias("first_known_segment"),
        F.lead("session_no", 2).over(w10).alias("next_session_no"),
        F.round((F.col("credit_limit") / F.sum("credit_limit").over(w11)), 4).alias("credit_limit_share"),
    )
)

kept_keys = (
    metrics
    .select("segment", "id")
    .intersect(
        df_customers
        .filter(F.upper("name").like("%IA %"))
        .select("segment", "id")
    )
)

reconciled = (
    metrics
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.segment") == F.col("k.segment")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select(
        "id",
        "segment",
        "age",
        "session_started_at",
        "session_device",
        "session_no",
        "next_session_no",
        "credit_limit_share",
    )
    .orderBy(F.col("session_started_at").desc_nulls_last())
)
