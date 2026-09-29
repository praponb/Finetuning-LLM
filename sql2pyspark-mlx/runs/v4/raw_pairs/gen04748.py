from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy(F.col("started_at").desc_nulls_last(), "id")
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device").orderBy(F.col("started_at").desc(), "id")
w_grp = Window.orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("device").orderBy("event_seq")
w6 = Window.partitionBy("country").orderBy("started_at_date", "id")
w_recent = Window.partitionBy("country")
w8 = Window.partitionBy("device").orderBy("next_country_length", "id").rowsBetween(-6, Window.currentRow)
w9 = Window.orderBy(F.col("started_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)

kept = (
    df_sessions
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.event_time") < "2024-10-22"), "left_anti")
)

ranked = (
    kept
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

enriched = (
    ranked
    .select(
        "*",
        F.first("country", ignorenulls=True).over(w2).alias("first_known_country"),
        F.row_number().over(w3).alias("event_seq"),
        F.last("browser", ignorenulls=True).over(w_grp).alias("last_known_browser"),
    )
)

latest = (
    enriched
    .withColumn("dedup_rank", F.rank().over(w5))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

cleaned = (
    latest
    .filter((F.col("started_at") >= F.to_timestamp(F.lit("2024-03-30 13:00:00"))) & F.col("browser").isNotNull())
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        "latest_rank",
        "first_known_country",
        "last_known_browser",
        F.to_date("started_at").alias("started_at_date"),
        F.trim("device").alias("clean_device"),
        F.coalesce("browser", F.lit("NONE")).alias("browser_clean"),
    )
)

cleaned_sessions = (
    cleaned
    .filter(F.col("device").isin(["mobile", "tablet"]))
    .select(
        "id",
        "device",
        "started_at",
        "country",
        "first_known_country",
        "last_known_browser",
        "started_at_date",
        "browser_clean",
        F.length("country").alias("country_length"),
        F.date_add("started_at_date", 68).alias("started_at_date_plus_68d"),
    )
)

scored = (
    cleaned_sessions
    .withColumn("next_country_length", F.lead("country_length", 1, 0).over(w6))
    .withColumn("country_stddev_samp_country_length", F.stddev_samp("country_length").over(w_recent))
)

result = (
    scored
    .withColumn("next_country_length_avg_6", F.avg("next_country_length").over(w8))
    .withColumn("country_length_sum_30d", F.sum("country_length").over(w9))
)
