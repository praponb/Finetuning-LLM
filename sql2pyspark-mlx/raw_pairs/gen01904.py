from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser", "country").orderBy(F.col("years_since_started_at").desc(), "id")
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("country", "customer_id").orderBy(F.col("years_since_started_at").desc_nulls_first())
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())

cleaned = (
    df_sessions
    .filter(F.col("started_at") > F.lit("2023-08-17 22:00:00").cast("timestamp"))
    .select(
        "id",
        "customer_id",
        "browser",
        "started_at",
        "country",
        (F.datediff(F.current_date(), "started_at") / 365).alias("years_since_started_at"),
        F.when(F.col("country").isin(["JP", "TH"]), "Tier 1").otherwise("Other").alias("country_group"),
        F.coalesce("country", F.lit("OTHER")).alias("resolved_country"),
    )
)

latest = (
    cleaned
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "customer_id",
        "started_at",
        "country",
        "years_since_started_at",
        "country_group",
    )
)

enriched = (
    latest
    .filter(F.col("country").isin(["US", "UK", "SG"]))
    .select(
        "id",
        "customer_id",
        "started_at",
        "country_group",
        F.sum("years_since_started_at").over(w2).alias("running_sum_years_since_started_at"),
        F.min("years_since_started_at").over(w2).alias("running_min_years_since_started_at"),
        F.avg("years_since_started_at").over(w2).alias("running_avg_years_since_started_at"),
        F.percent_rank().over(w3).alias("years_since_started_at_percent_rank"),
    )
)

latest_match = (
    enriched
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.event_type"), F.col("c.duration_sec"))
)

result = (
    latest_match
    .select(
        "id",
        "customer_id",
        "started_at",
        "running_min_years_since_started_at",
        "running_avg_years_since_started_at",
        "years_since_started_at_percent_rank",
        "event_type",
        "duration_sec",
        F.when(F.col("event_type").rlike("^(search|click)$"), "EXTENDED").when(F.col("event_type").rlike("^(page_view|add_to_cart)$"), "GROUP_B").otherwise("CORE").alias("event_type_zone"),
    )
    .orderBy("event_type_zone")
)
