from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("browser").orderBy(F.col("started_at").desc(), "id")
w3 = Window.partitionBy(F.month("started_at"), "browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("customer_id").orderBy("started_at", "id")
w5 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("segment", "browser").orderBy("seq_no_plus_event_seq", "id")
w7 = Window.partitionBy("segment", "country").orderBy(F.col("next_seq_no").desc(), "id")

enriched = (
    df_sessions
    .select(
        "id",
        "customer_id",
        "browser",
        F.last("browser", ignorenulls=True).over(w).alias("last_known_browser"),
        F.row_number().over(w2).alias("event_seq"),
        F.last("country", ignorenulls=True).over(w3).alias("last_known_country"),
        F.row_number().over(w4).alias("seq_no"),
        F.first("country", ignorenulls=True).over(w5).alias("first_known_country"),
    )
)

enriched_sessions = (
    enriched
    .alias("src")
    .join(df_customers.alias("b"), F.col("src.customer_id") == F.col("b.id"), "inner")
    .select("src.*", F.col("b.segment"), F.col("b.country"))
)

kept = (
    enriched_sessions
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.event_type") == "page_view"), "left_anti")
)

filtered = (
    kept
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "browser",
        "last_known_country",
        "seq_no",
        "first_known_country",
        "segment",
        "country",
        (F.col("seq_no") + F.col("event_seq")).alias("seq_no_plus_event_seq"),
        F.concat_ws(",", "browser", "country").alias("browser_country_key"),
    )
)

enriched_sessions_2 = (
    filtered
    .select(
        "*",
        F.lead("seq_no").over(w6).alias("next_seq_no"),
        F.lead("seq_no_plus_event_seq").over(w6).alias("next_seq_no_plus_event_seq"),
        F.lag("seq_no_plus_event_seq").over(w6).alias("prev_seq_no_plus_event_seq"),
        F.row_number().over(w6).alias("event_seq"),
    )
)

latest = (
    enriched_sessions_2
    .withColumn("row_num", F.row_number().over(w7))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest
    .groupBy(
        F.when(F.col("prev_seq_no_plus_event_seq") >= 73, "LARGE").otherwise("SMALL").alias("prev_seq_no_plus_event_seq_tier"),
        "country",
        F.when(F.col("seq_no_plus_event_seq") >= 65, "HIGH").otherwise("LOW").alias("seq_no_plus_event_seq_tier"),
    )
    .agg(
        F.sum(F.when(F.col("segment") == "Consumer", F.col("seq_no_plus_event_seq")).otherwise(0)).alias("consumer_seq_no_plus_event_seq"),
        F.percentile_approx("seq_no_plus_event_seq", [0.25, 0.5, 0.75]).alias("seq_no_plus_event_seq_quartiles"),
        F.percentile_approx("next_seq_no_plus_event_seq", [0.25, 0.5, 0.75]).alias("next_seq_no_plus_event_seq_quartiles"),
        F.round(F.sum("next_seq_no_plus_event_seq") / F.nullif(F.count(F.when(F.col("browser") == "Firefox", 1)), F.lit(0)), 2).alias("next_seq_no_plus_event_seq_per_firefox"),
    )
)
