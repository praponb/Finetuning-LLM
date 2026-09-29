from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("segment").orderBy("age", "id")
w_hist = Window.partitionBy("segment").orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.opened_date").desc(), F.col("c.id").desc())
w_running = Window.partitionBy("status", "city").orderBy("row_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("city").orderBy("next_credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.started_at").desc(), F.col("c.id").desc())

metrics = (
    df_customers
    .select(
        "id",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "credit_limit",
        "age",
        F.lead("age").over(w_recent).alias("next_age"),
        F.lead("credit_limit").over(w_recent).alias("next_credit_limit"),
        F.lag("credit_limit").over(w_recent).alias("prev_credit_limit"),
        F.row_number().over(w_recent).alias("row_seq"),
        F.last("segment", ignorenulls=True).over(w_hist).alias("last_known_segment"),
    )
)

eligible = (
    metrics
    .alias("s")
    .join(df_subscriptions.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.end_date") <= F.col("s.signup_date")), "left_anti")
)

with_latest = (
    eligible
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.opened_date") <= F.col("s.signup_date")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.status"))
)

enriched = (
    with_latest
    .select(
        "id",
        "age",
        "next_age",
        "next_credit_limit",
        "last_known_segment",
        F.last("next_age", ignorenulls=True).over(w_running).alias("last_known_next_age"),
        F.sum("prev_credit_limit").over(w5).alias("running_prev_credit_limit"),
    )
)

with_last = (
    enriched
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.country").alias("session_country"), F.col("c.device"))
)

result = (
    with_last
    .select(
        "id",
        "age",
        "next_age",
        "last_known_segment",
        "last_known_next_age",
        "running_prev_credit_limit",
        "session_country",
        "device",
        F.when(F.col("device").rlike("^(mobile|desktop)$"), "OTHER_ZONE").otherwise("CORE").alias("device_zone"),
    )
)
