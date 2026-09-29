from pyspark.sql import functions as F

eligible = (
    df_sessions
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_type") != "add_to_cart"), "left_semi")
)

joined = (
    eligible
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id").eqNullSafe(F.col("c.id")), "left")
    .select("e.id", "e.customer_id", "e.browser", F.col("c.segment"))
)

not_flagged = (
    joined
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") < "2024-12-01"), "left_anti")
)

eligible_sessions = (
    not_flagged
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.event_type").isin(["add_to_cart", "search"])), "left_anti")
)

result = (
    eligible_sessions
    .groupBy("browser")
    .agg(
        F.sort_array(F.collect_set("segment")).alias("segment_list"),
        F.size(F.collect_set("segment")).alias("num_segments"),
        F.countDistinct("browser").alias("distinct_browsers"),
    )
    .filter(F.col("num_segments") < 1000)
    .orderBy(F.col("num_segments").desc())
)
