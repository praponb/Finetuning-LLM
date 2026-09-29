from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_sessions
    .filter(F.col("country").isNotNull())
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        F.to_date("started_at").alias("started_at_date"),
        F.trim("device").alias("clean_device"),
        F.coalesce("country", F.lit("DIRECT")).alias("resolved_country"),
    )
)

with_gaps = (
    filtered
    .withColumn("is_new_session", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

sessions_sessions = (
    with_gaps
    .select(
        "id",
        "customer_id",
        "browser",
        "started_at",
        "country",
        "clean_device",
        "resolved_country",
        F.sum("is_new_session").over(w2).alias("session_id"),
    )
)

result = (
    sessions_sessions
    .withColumn("year_start", F.date_trunc("year", "started_at"))
    .withColumn("session_id_tier", F.when(F.col("session_id") >= 6, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["year_start", "session_id_tier"], ["year_start"], []],
        "year_start",
        "session_id_tier",
    )
    .agg(
        F.count_if(F.col("country") == "DE").alias("de_events"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.max("started_at").alias("ended_at"),
        F.min("started_at").alias("first_seen"),
        F.sum("session_id").alias("total_session_id"),
        F.count("*").alias("row_count"),
        F.grouping_id().alias("grouping_level"),
    )
)
