from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("started_at", "id")
w2 = Window.partitionBy("device").orderBy(F.col("started_at").desc(), "id")
w3 = Window.partitionBy("customer_id").orderBy("started_at", "id")
w4 = Window.partitionBy("browser").orderBy(F.col("us_events").desc())

metrics = (
    df_sessions
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("row_seq", F.row_number().over(w2))
    .withColumn("event_seq", F.row_number().over(w3))
)

eligible = (
    metrics
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.event_time") <= F.col("s.started_at")), "left_anti")
)

eligible_sessions = (
    eligible
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.to_date(F.col("x.event_time")) < "2025-10-04"), "left_semi")
)

base = (
    eligible_sessions
    .filter(F.col("started_at") >= F.lit("2023-03-19").cast("date"))
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "country",
        "seq_no",
        "row_seq",
        "event_seq",
        F.least("seq_no", "row_seq").alias("least_seq_no_row_seq"),
        F.hour("started_at").alias("started_at_hour"),
        F.coalesce("country", F.lit("UNKNOWN")).alias("country_clean"),
    )
)

joined = (
    base
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.age"), F.col("d.email"), F.col("d.credit_limit"))
)

result = (
    joined
    .groupBy("browser", "device")
    .agg(
        F.count(F.when(F.col("country") == "UK", 1)).alias("uk_count"),
        F.size(F.collect_set("device")).alias("num_devices"),
        F.count_if(F.col("country") == "US").alias("us_events"),
    )
    .withColumn("us_events_percent_rank", F.percent_rank().over(w4))
    .orderBy(F.col("browser").desc())
)
