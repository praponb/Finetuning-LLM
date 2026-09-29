from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "browser").orderBy(F.col("started_at").desc(), "id")
w2 = Window.partitionBy("country", "browser").orderBy("started_at", "id")
w3 = Window.partitionBy("device").orderBy("event_seq", "id")
w4 = Window.partitionBy("country")
w5 = Window.partitionBy("device", "country").orderBy("event_seq", "id")
w6 = Window.partitionBy("browser").orderBy("event_seq_2", "id").rowsBetween(-3, -1)
w7 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())
discount_by_customer_id = df_orders.groupBy("customer_id").agg(F.avg("discount").alias("avg_order_discount"))

base = (
    df_sessions
    .filter((F.col("started_at") > F.lit("2024-01-30").cast("date")) & (F.col("customer_id") == 16))
    .withColumn("browser_prefix", F.substring("browser", 1, 3))
)

with_windows = (
    base
    .select(
        "*",
        F.row_number().over(w).alias("event_seq"),
        F.row_number().over(w2).alias("event_seq_2"),
    )
)

latest = (
    with_windows
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
)

with_windows_sessions = (
    latest
    .withColumn("event_seq_share", F.round((F.col("event_seq") / F.sum("event_seq").over(w4)), 4))
    .withColumn("next_event_seq", F.lead("event_seq", 2).over(w5))
    .withColumn("event_seq_avg_3_prior", F.avg("event_seq").over(w6))
)

latest_match = (
    with_windows_sessions
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.event_type").alias("web_event_event_type"),
        F.col("c.page").alias("web_event_page"),
    )
)

enriched = (
    latest_match
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "inner")
    .select("a.*", F.col("p.credit_limit"), F.col("p.signup_date"))
)

result = (
    enriched
    .join(discount_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "browser_prefix",
        "event_seq_2",
        "event_seq_share",
        "next_event_seq",
        F.col("avg_order_discount"),
    )
    .orderBy("avg_order_discount", F.col("event_seq_2").desc())
)
