from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at", "id")
w2 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())
w4 = Window.partitionBy("device").orderBy(F.col("started_at").desc_nulls_last(), "id")

filtered = (
    df_sessions
    .filter((F.col("started_at") >= F.to_date(F.lit("2023-04-14"))) & F.col("customer_id").isNotNull())
    .select(
        "*",
        F.trim("device").alias("clean_device"),
        F.when(F.col("browser") == "Firefox", 1).otherwise(0).alias("is_firefox"),
    )
)

with_gaps = (
    filtered
    .withColumn("session_start_flag", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("session_id", F.sum("session_start_flag").over(w2))

with_last = (
    sessionized
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.device",
        "s.browser",
        "s.started_at",
        "s.country",
        "s.is_firefox",
        "s.session_id",
        F.col("c.page"),
    )
)

ranked = (
    with_last
    .withColumn("row_num", F.row_number().over(w4))
    .filter(F.col("row_num") == 1)
)

eligible = (
    ranked
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") <= F.col("s.started_at")), "left_anti")
)

result = (
    eligible
    .groupBy(
        F.when(F.col("session_id") >= 6, "HIGH").otherwise("LOW").alias("session_id_tier"),
    )
    .agg(
        F.min("is_firefox").alias("min_is_firefox"),
        F.percentile_approx("is_firefox", 0.5).alias("is_firefox_p50"),
        F.max("started_at").alias("ended_at"),
        F.count(F.when(F.col("page") == "/checkout", 1)).alias("checkout_rows"),
    )
    .orderBy(F.col("is_firefox_p50").desc_nulls_last())
)
