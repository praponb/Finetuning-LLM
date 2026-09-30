from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser", "country").orderBy(F.col("started_at").desc_nulls_last(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("device").orderBy("duration_sec")
w4 = Window.partitionBy("country").orderBy(F.col("started_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)

latest = (
    df_sessions
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

with_last = (
    latest
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") <= F.col("s.started_at")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.duration_sec"))
)

cleaned = (
    with_last
    .filter(F.col("country").isNotNull() & (F.col("device") == "desktop"))
    .select(
        "*",
        (F.datediff(F.current_date(), "started_at") / 365.25).alias("years_since_started_at"),
        F.coalesce("country", F.lit("UNKNOWN")).alias("country_filled"),
    )
)

scored = (
    cleaned
    .filter(F.hour("started_at").between(11, 19))
    .withColumn("duration_sec_percent_rank", F.percent_rank().over(w3))
    .withColumn("years_since_started_at_max_30d", F.max("years_since_started_at").over(w4))
)

kept = (
    scored
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.session_id") == 16), "left_anti")
)

cleaned_sessions = (
    kept
    .filter((F.col("started_at") > F.lit("2023-05-29 08:00:00").cast("timestamp")) & F.col("customer_id").isNotNull())
    .select(
        "*",
        F.round((F.col("years_since_started_at") * 1.15), 2).alias("years_since_started_at_adjusted"),
        F.coalesce("country", F.lit("DIRECT")).alias("country_filled_2"),
    )
)

result = (
    cleaned_sessions
    .select(
        "id",
        "country",
        "years_since_started_at",
        "country_filled",
        "years_since_started_at_max_30d",
        "years_since_started_at_adjusted",
        "country_filled_2",
        F.when(F.col("country").rlike("^(UK|SG)$"), "GROUP_A").when(F.col("country").rlike("^(TH|US)$"), "GROUP_B").otherwise("PRIORITY").alias("country_zone"),
    )
    .orderBy("years_since_started_at")
)
