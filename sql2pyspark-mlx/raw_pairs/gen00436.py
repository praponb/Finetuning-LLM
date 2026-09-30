from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("browser").orderBy("browser_length", "id")
w2 = Window.partitionBy("browser").orderBy("started_at", "id")
w3 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_sessions
    .filter(~F.col("country").isin(["US", "JP"]))
    .select(
        "id",
        "customer_id",
        "browser",
        "started_at",
        F.length("browser").alias("browser_length"),
        F.coalesce("browser", F.lit("DIRECT")).alias("browser_filled"),
    )
)

scored = (
    base
    .filter(F.col("customer_id").isNull())
    .withColumn("next_browser_length", F.lead("browser_length").over(w_grp))
    .withColumn("prev_browser_length", F.lag("browser_length").over(w_grp))
    .withColumn("seq_no", F.row_number().over(w_grp))
)

not_flagged = (
    scored
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.duration_sec") <= 305), "left_anti")
)

with_prev = not_flagged.withColumn("prev_started_at", F.lag("started_at").over(w2))

gaps = (
    with_prev
    .withColumn("session_start_flag", F.when(F.col("prev_started_at").isNull() | (F.col("started_at") > F.col("prev_started_at") + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessionized = gaps.withColumn("session_no", F.sum("session_start_flag").over(w3))

result = (
    sessionized
    .cube(
        "browser",
        F.when(F.col("session_start_flag") >= 1, "PREMIUM").otherwise("STANDARD").alias("session_start_flag_tier"),
    )
    .agg(
        F.count_if(F.col("browser") == "Chrome").alias("chrome_events"),
        F.count("*").alias("cnt"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("span_minutes"),
        F.max("started_at").alias("last_started_at"),
    )
    .orderBy(F.col("span_minutes").desc_nulls_last())
)
