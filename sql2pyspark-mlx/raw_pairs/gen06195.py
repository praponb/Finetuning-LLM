from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at", "id")
w2 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("device").orderBy("session_no")
w4 = Window.partitionBy(F.year("started_at"), "country").orderBy("session_start_flag", "id")
w5 = Window.partitionBy("customer_id").orderBy("next_session_start_flag", "id")
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())
discount_by_customer_id = df_orders.groupBy("customer_id").agg(F.max("discount").alias("max_order_discount"))

not_flagged = (
    df_sessions
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.event_time") <= F.col("s.started_at")), "left_anti")
)

flagged = (
    not_flagged
    .withColumn("session_start_flag", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessions_sessions = flagged.withColumn("session_no", F.sum("session_start_flag").over(w2))

scored = (
    sessions_sessions
    .filter(F.col("started_at") < "2025-09-01")
    .select(
        "*",
        F.dense_rank().over(w_ordered).alias("session_no_dense_rank"),
        F.lead("session_start_flag", 1).over(w4).alias("next_session_start_flag"),
    )
)

scored_sessions = (
    scored
    .select(
        "id",
        "customer_id",
        "country",
        "next_session_start_flag",
        F.lead("session_no").over(w5).alias("next_session_no"),
        F.lag("session_no").over(w5).alias("prev_session_no"),
        F.lag("session_start_flag").over(w5).alias("prev_session_start_flag"),
        F.row_number().over(w5).alias("event_seq"),
    )
)

latest_match = (
    scored_sessions
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.duration_sec"), F.col("c.event_time"))
)

result = (
    latest_match
    .join(discount_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "next_session_start_flag",
        "next_session_no",
        "prev_session_no",
        "prev_session_start_flag",
        "event_seq",
        "event_time",
        F.col("max_order_discount"),
    )
)
