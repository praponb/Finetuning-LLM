from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("signup_date"), "customer_id").orderBy("started_at", "id")
w2 = Window.partitionBy("device").orderBy(F.col("signup_date").desc(), "id")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())
w4 = Window.partitionBy("customer_id")
w5 = Window.partitionBy("country").orderBy("seq_no_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

joined = (
    df_sessions
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.year(F.col("c.signup_date")) == 2024)
    .select("s.*", F.col("c.email"))
)

filtered = (
    joined
    .filter(F.col("customer_id").isNotNull())
    .withColumn("device_group", F.when(F.col("device").isin(["mobile", "tablet"]), "Core").otherwise("Other"))
)

kept = (
    filtered
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.event_time") >= F.col("s.started_at")), "left_anti")
)

enriched = (
    kept
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id").eqNullSafe(F.col("p.id")), "left")
    .select("src.*", F.col("p.signup_date"))
)

enriched_sessions = (
    enriched
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "country",
        "email",
        F.row_number().over(w).alias("seq_no"),
        F.row_number().over(w2).alias("seq_no_2"),
    )
)

with_last = (
    enriched_sessions
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.duration_sec"))
)

with_windows = (
    with_last
    .select(
        "*",
        F.avg("duration_sec").over(w4).alias("customer_id_avg_duration_sec"),
        F.sum("seq_no").over(w5).alias("running_seq_no"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "device",
        "browser",
        "email",
        "seq_no",
        "seq_no_2",
        "duration_sec",
        "running_seq_no",
    )
)
