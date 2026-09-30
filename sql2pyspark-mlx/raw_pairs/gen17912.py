from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "browser").orderBy("started_at_date", "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())

valid = (
    df_sessions
    .filter(F.col("browser").isNotNull() & (F.col("country").eqNullSafe(F.lit("UK"))))
    .select(
        "*",
        F.to_date("started_at").alias("started_at_date"),
        F.initcap(F.trim("browser")).alias("browser_title"),
    )
)

deduped = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "customer_id", "device", "browser", "started_at", "country")
)

with_last = (
    deduped
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") <= F.col("s.started_at")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.duration_sec"), F.col("c.page"))
)

not_flagged = (
    with_last
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.event_time") < F.col("s.started_at")), "left_semi")
)

result = (
    not_flagged
    .rollup(
        F.when(F.col("duration_sec") >= 476, "LARGE").otherwise("SMALL").alias("duration_sec_tier"),
        F.date_trunc("month", "started_at").alias("activity_month"),
    )
    .agg(
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("duration_min"),
        F.count("*").alias("num_rows"),
        F.sum(F.when(F.col("page") == "/product", F.col("duration_sec")).otherwise(0)).alias("product_duration_sec"),
    )
    .filter(F.col("duration_min") > 0)
    .orderBy(F.col("duration_min").desc_nulls_last())
)
