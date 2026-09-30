from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at", "id")
w2 = Window.partitionBy("country").orderBy(F.col("started_at").desc(), "id")
w3 = Window.partitionBy("country", "device").orderBy("started_at", "id")
w4 = Window.partitionBy("customer_id").orderBy(F.col("seq_no").desc())

cleaned = (
    df_sessions
    .filter(F.col("customer_id").isNotNull() & (F.col("device") == "tablet"))
    .select(
        "*",
        F.upper("browser").alias("browser_upper"),
        F.coalesce("country", F.lit("OTHER")).alias("resolved_country"),
    )
)

enriched = (
    cleaned
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("row_seq", F.row_number().over(w2))
    .withColumn("event_seq", F.row_number().over(w3))
)

deduped = (
    enriched
    .withColumn("rank_in_group", F.rank().over(w4))
    .filter(F.col("rank_in_group") == 1)
)

filtered = (
    deduped
    .filter((F.col("started_at") >= F.to_timestamp(F.lit("2024-03-19 14:00:00"))) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "browser",
        "started_at",
        "country",
        "resolved_country",
        "seq_no",
        "row_seq",
        "rank_in_group",
        F.when(F.col("device").isin(["mobile", "desktop"]), "Group A").otherwise("Other").alias("device_group"),
        F.initcap(F.trim("browser")).alias("browser_title"),
    )
)

not_flagged = (
    filtered
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") <= F.col("s.started_at")), "left_anti")
)

result = (
    not_flagged
    .groupBy(F.date_trunc("QUARTER", "started_at").alias("quarter_start"))
    .agg(
        F.max("started_at").alias("last_started_at"),
        F.sum(F.when(F.col("browser") == "Safari", F.col("seq_no")).otherwise(0)).alias("safari_seq_no"),
    )
    .orderBy("safari_seq_no")
)
