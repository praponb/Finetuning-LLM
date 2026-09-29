from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("started_at").asc_nulls_first())

ranked = (
    df_sessions
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "browser", "started_at")
)

kept = (
    ranked
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.event_time") < F.col("s.started_at")), "left_anti")
)

eligible = (
    kept
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.hour(F.col("ch.event_time")).between(11, 19)), "left_anti")
)

kept_sessions = (
    eligible
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") < F.col("s.started_at")), "left_anti")
)

result = (
    kept_sessions
    .groupBy("browser")
    .agg(
        F.countDistinct("browser").alias("distinct_browsers"),
        F.sort_array(F.collect_set("browser")).alias("browser_list"),
        F.count(F.when(F.col("browser") == "Safari", 1)).alias("safari_count"),
        F.min("started_at").alias("first_started_at"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("duration_min"),
        F.min("started_at").alias("started_at"),
    )
    .filter(F.col("duration_min") < 1000000)
    .orderBy(F.col("browser").desc())
)
