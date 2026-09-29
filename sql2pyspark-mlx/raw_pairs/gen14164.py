from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("customer_id").orderBy("signup_date", "id")
w2 = Window.partitionBy("customer_id").orderBy(F.col("signup_date").desc(), "id")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())

cleaned = (
    df_sessions
    .filter((F.col("started_at") >= F.lit("2023-02-20 05:00:00").cast("timestamp")) & F.col("browser").isNotNull())
    .select(
        "id",
        "customer_id",
        F.coalesce("browser", F.lit("Unknown")).alias("browser_filled"),
    )
)

with_dims = (
    cleaned
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .select("s.*", F.col("c.name"))
)

enriched = (
    with_dims
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.signup_date"), F.col("p.name").alias("customer_name"))
)

with_windows = (
    enriched
    .filter(F.upper("name").like("%ANY%"))
    .select(
        "id",
        "browser_filled",
        F.row_number().over(w_hist).alias("seq_no"),
        F.row_number().over(w_hist).alias("seq_no_2"),
        F.row_number().over(w2).alias("seq_no_3"),
    )
)

with_latest = (
    with_windows
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.browser_filled",
        "s.seq_no_2",
        "s.seq_no_3",
        F.col("c.event_type").alias("web_event_event_type"),
    )
)

result = (
    with_latest
    .select("id", "browser_filled", "seq_no_2", "seq_no_3")
    .orderBy(F.col("id").desc_nulls_last())
)
