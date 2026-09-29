from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("promo_code").orderBy("discount_pct_int", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("promo_code").orderBy(F.col("starts_on_weekofyear").desc(), "id")
w3 = Window.orderBy("running_max_discount_pct_int", "id")
w4 = Window.orderBy(F.col("running_max_discount_pct_int").desc(), "id")
w5 = Window.orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.orderBy("running_max_discount_pct_int", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy(F.col("running_count_discount_pct_int").desc(), "id")

valid = (
    df_promotions
    .filter((F.col("ends_on") > F.lit("2023-07-09").cast("date")) & F.col("channel").isNotNull())
    .select(
        "id",
        "promo_code",
        "starts_on",
        "ends_on",
        F.col("discount_pct").cast("int").alias("discount_pct_int"),
        F.coalesce("channel", F.lit("N/A")).alias("channel_clean"),
    )
)

filtered = (
    valid
    .filter((F.col("ends_on") >= F.lit("2023-06-07").cast("date")) & (F.year("starts_on") >= 2025))
    .withColumn("starts_on_weekofyear", F.weekofyear("starts_on"))
)

with_windows = (
    filtered
    .select(
        "id",
        "starts_on",
        "ends_on",
        F.min("discount_pct_int").over(w_running).alias("running_min_discount_pct_int"),
        F.max("discount_pct_int").over(w_running).alias("running_max_discount_pct_int"),
        F.count("discount_pct_int").over(w_running).alias("running_count_discount_pct_int"),
        F.avg("discount_pct_int").over(w_running).alias("running_avg_discount_pct_int"),
        F.ntile(10).over(w2).alias("starts_on_weekofyear_bucket"),
    )
)

with_windows_promotions = (
    with_windows
    .select(
        "*",
        F.row_number().over(w3).alias("event_seq"),
        F.row_number().over(w4).alias("event_seq_2"),
        F.first("running_max_discount_pct_int", ignorenulls=True).over(w5).alias("first_known_running_max_discount_pct_int"),
        F.last("running_avg_discount_pct_int", ignorenulls=True).over(w_recent).alias("last_known_running_avg_discount_pct_int"),
        F.ntile(3).over(w7).alias("running_count_discount_pct_int_bucket"),
    )
)

metrics = (
    with_windows_promotions
    .filter(F.col("running_min_discount_pct_int").between(67, 78))
    .select(
        "id",
        "starts_on",
        "running_min_discount_pct_int",
        "running_max_discount_pct_int",
        "running_avg_discount_pct_int",
        "event_seq_2",
        "first_known_running_max_discount_pct_int",
        "running_count_discount_pct_int_bucket",
        F.min("event_seq").over(w_recent).alias("running_min_event_seq"),
        F.count("event_seq").over(w_recent).alias("running_count_event_seq"),
        F.avg("event_seq").over(w_recent).alias("running_avg_event_seq"),
    )
)

base = (
    metrics
    .filter(F.col("starts_on") >= F.lit("2023-06-20").cast("date"))
    .select(
        "id",
        "starts_on",
        "running_avg_discount_pct_int",
        "event_seq_2",
        "first_known_running_max_discount_pct_int",
        "running_count_discount_pct_int_bucket",
        "running_count_event_seq",
        "running_avg_event_seq",
        (F.col("running_min_event_seq") + F.col("running_max_discount_pct_int")).alias("running_min_event_seq_plus_running_max_discount_pct_int"),
    )
)

result = (
    base
    .groupBy(F.date_trunc("WEEK", "starts_on").alias("week_start"))
    .agg(
        F.count("*").alias("row_count"),
        F.percentile_approx("running_avg_event_seq", 0.5).alias("running_avg_event_seq_p50"),
    )
    .orderBy("running_avg_event_seq_p50")
)
