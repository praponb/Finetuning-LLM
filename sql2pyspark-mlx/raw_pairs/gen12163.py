from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("customer_id").orderBy(F.col("device_length").asc_nulls_first(), "id")

base = (
    df_sessions
    .filter(F.col("country").isNotNull())
    .select("*", F.length("device").alias("device_length"))
)

latest_match = (
    base
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.event_time").alias("web_event_event_time"),
        F.col("c.event_type").alias("web_event_event_type"),
    )
)

deduped = (
    latest_match
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("id", "customer_id", "started_at", "country")
)

eligible = (
    deduped
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.event_time") >= F.col("s.started_at")), "left_semi")
)

not_flagged = (
    eligible
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.event_time") >= F.col("s.started_at")), "left_semi")
)

totals = (
    not_flagged
    .groupBy("country", F.date_trunc("WEEK", "started_at").alias("week_start"))
    .agg(
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.count(F.when(F.col("country") == "US", 1)).alias("us_count"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)

result = (
    totals
    .select(
        "country",
        "week_start",
        "us_count",
        "distinct_ids",
        F.when(F.col("country").rlike("^(UK|TH)$"), "GROUP_A").otherwise("EXTENDED").alias("country_zone"),
    )
    .orderBy(F.col("country_zone").desc_nulls_last())
)
