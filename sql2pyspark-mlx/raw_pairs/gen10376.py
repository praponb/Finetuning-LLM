from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")
w2 = Window.partitionBy(F.date_trunc("YEAR", "started_at"), "country").orderBy("is_mobile", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("browser").orderBy("running_is_mobile", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("city").orderBy("next_is_mobile", "id")
w5 = Window.partitionBy("city", "country")
w6 = Window.partitionBy("device", "browser")
w7 = Window.partitionBy("city", "device").orderBy("seq_no")

base = (
    df_sessions
    .filter((F.col("started_at") > F.lit("2023-07-30 16:00:00").cast("timestamp")) & F.col("country").isNotNull())
    .select(
        "*",
        F.when(F.col("device") == "mobile", 1).otherwise(0).alias("is_mobile"),
        F.coalesce("country", F.lit("DIRECT")).alias("country_filled"),
    )
)

kept = (
    base
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.page") != "/checkout"), "left_semi")
)

enriched = (
    kept
    .filter(F.col("is_mobile") == 31)
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        "country_filled",
        F.lead("is_mobile").over(w).alias("next_is_mobile"),
        F.lag("is_mobile").over(w).alias("prev_is_mobile"),
        F.row_number().over(w).alias("seq_no"),
        F.sum("is_mobile").over(w2).alias("running_is_mobile"),
    )
)

enriched_sessions = (
    enriched
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.name"), F.col("d.city"))
)

metrics = (
    enriched_sessions
    .select(
        "id",
        "device",
        "browser",
        "country",
        "country_filled",
        "prev_is_mobile",
        "seq_no",
        "running_is_mobile",
        "city",
        F.min("prev_is_mobile").over(w_part).alias("running_min_prev_is_mobile"),
        F.max("prev_is_mobile").over(w_part).alias("running_max_prev_is_mobile"),
        F.avg("prev_is_mobile").over(w_part).alias("running_avg_prev_is_mobile"),
        F.sum("prev_is_mobile").over(w_part).alias("running_sum_prev_is_mobile"),
        F.lead("running_is_mobile", 2).over(w4).alias("next_running_is_mobile"),
    )
)

with_windows = (
    metrics
    .withColumn("city_avg_running_max_prev_is_mobile", F.avg("running_max_prev_is_mobile").over(w5))
    .withColumn("device_stddev_samp_running_sum_prev_is_mobile", F.stddev_samp("running_sum_prev_is_mobile").over(w6))
)

latest = (
    with_windows
    .withColumn("rn", F.rank().over(w7))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest
    .select(
        "id",
        "browser",
        "country_filled",
        "prev_is_mobile",
        "seq_no",
        "running_min_prev_is_mobile",
        "running_max_prev_is_mobile",
        "running_avg_prev_is_mobile",
        "running_sum_prev_is_mobile",
    )
    .orderBy(F.col("running_sum_prev_is_mobile").desc())
)
