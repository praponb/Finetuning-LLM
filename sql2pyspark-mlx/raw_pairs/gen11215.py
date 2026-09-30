from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("country_length").desc_nulls_first())
w_grp = Window.partitionBy("country", "customer_id").orderBy("is_tablet", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("customer_id").orderBy(F.col("is_tablet").desc_nulls_last())
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())
w5 = Window.partitionBy("web_event_page")
w6 = Window.partitionBy("web_event_page").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy(F.col("started_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w8 = Window.orderBy("country_length_rank_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_sessions
    .filter((F.col("started_at") > F.lit("2023-08-30 10:00:00").cast("timestamp")) & (F.col("country") == "DE"))
    .select(
        "id",
        "customer_id",
        "started_at",
        "country",
        F.when(F.col("device") == "tablet", 1).otherwise(0).alias("is_tablet"),
        F.length("country").alias("country_length"),
    )
)

with_windows = (
    base
    .select(
        "*",
        F.rank().over(w).alias("country_length_rank"),
        F.sum("country_length").over(w_grp).alias("running_country_length"),
        F.rank().over(w3).alias("is_tablet_rank"),
    )
)

with_last = (
    with_windows
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.started_at",
        "s.country_length",
        "s.country_length_rank",
        "s.running_country_length",
        "s.is_tablet_rank",
        F.col("c.duration_sec"),
        F.col("c.page").alias("web_event_page"),
    )
)

with_windows_sessions = (
    with_last
    .filter(F.col("web_event_page").isin(["/search", "/product"]))
    .select(
        "id",
        "started_at",
        "country_length",
        "running_country_length",
        "is_tablet_rank",
        "duration_sec",
        F.round((F.col("country_length_rank") / F.sum("country_length_rank").over(w5)), 4).alias("country_length_rank_share"),
        F.sum("duration_sec").over(w6).alias("running_duration_sec"),
        F.max("running_country_length").over(w7).alias("running_country_length_max_30d"),
    )
)

result = (
    with_windows_sessions
    .select(
        "id",
        "started_at",
        "country_length",
        "is_tablet_rank",
        "running_country_length_max_30d",
        F.sum("running_country_length_max_30d").over(w8).alias("running_sum_running_country_length_max_30d"),
        F.count("running_country_length_max_30d").over(w8).alias("running_count_running_country_length_max_30d"),
        F.max("running_country_length_max_30d").over(w8).alias("running_max_running_country_length_max_30d"),
        F.avg("running_country_length_max_30d").over(w8).alias("running_avg_running_country_length_max_30d"),
    )
)
