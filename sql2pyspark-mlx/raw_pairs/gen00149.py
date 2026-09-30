from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("browser").orderBy(F.col("started_at_date").desc_nulls_last())
w3 = Window.partitionBy("customer_id").orderBy("started_at", "id")
w4 = Window.partitionBy("web_event_page").orderBy("latest_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("web_event_page").orderBy(F.col("started_at").desc(), "id")
w6 = Window.partitionBy(F.date_trunc("week", "started_at_date"), "browser").orderBy(F.col("started_at_date").desc(), "id")
w7 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_sessions
    .filter((F.col("started_at") >= F.to_timestamp(F.lit("2023-07-20 03:00:00"))) & (F.col("started_at") < "2025-01-01"))
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        F.to_date("started_at").alias("started_at_date"),
        F.coalesce("browser", F.lit("OTHER")).alias("browser_clean"),
    )
)

with_last = (
    base
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") <= F.col("s.started_at")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.page").alias("web_event_page"))
)

filtered = (
    with_last
    .filter((F.col("started_at") > F.lit("2023-03-19").cast("date")) & F.col("web_event_page").isNotNull())
    .withColumn("browser_prefix", F.substring("browser", 1, 3))
    .withColumn("country_clean", F.coalesce("country", F.lit("OTHER")))
)

latest = (
    filtered
    .withColumn("latest_rank", F.rank().over(w2))
    .filter(F.col("latest_rank") == 1)
)

result = (
    latest
    .filter(F.col("browser") != "Edge")
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "started_at_date",
        "browser_clean",
        "web_event_page",
        F.row_number().over(w3).alias("row_seq"),
        F.first("web_event_page", ignorenulls=True).over(w4).alias("first_known_web_event_page"),
        F.row_number().over(w5).alias("seq_no"),
        F.row_number().over(w6).alias("event_seq"),
        F.first("country", ignorenulls=True).over(w7).alias("first_known_country"),
    )
)
