from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("started_at").desc_nulls_first(), "id")
w2 = Window.orderBy(F.col("started_at").desc(), "id")

ranked = (
    df_sessions
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "customer_id", "device", "started_at", "country")
)

enriched = ranked.withColumn("row_seq", F.row_number().over(w2))

kept_keys = (
    enriched
    .select("customer_id", "device")
    .intersect(
        df_sessions
        .filter(F.col("country").isin(["JP", "UK"]))
        .select("customer_id", "device")
    )
)

reconciled = (
    enriched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.device") == F.col("k.device")), "inner")
    .select("s.*")
)

kept = (
    reconciled
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.event_time") <= F.col("s.started_at")), "left_anti")
)

eligible = (
    kept
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.duration_sec") != 266), "left_anti")
)

with_dims = (
    eligible
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "left")
    .filter(F.col("c.city") == "London")
    .select("src.*", F.col("c.name"), F.col("c.signup_date"))
)

matched_keys = (
    with_dims
    .select("country", "device")
    .intersect(
        df_sessions
        .filter(F.col("browser") == "Firefox")
        .select("country", "device")
    )
)

kept_rows = (
    with_dims
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.country") == F.col("k.country")) & (F.col("s.device") == F.col("k.device")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy("customer_id")
    .agg(
        F.max(F.when(F.col("country") == "DE", F.col("row_seq"))).alias("de_max_row_seq"),
        F.count(F.when(F.col("device") == "mobile", 1)).alias("mobile_rows"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("span_minutes"),
        F.min("started_at").alias("first_started_at"),
        F.min("started_at").alias("started_at"),
    )
    .orderBy(F.col("span_minutes").asc_nulls_last())
)
