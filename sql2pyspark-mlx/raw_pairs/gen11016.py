from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id", "event_type").orderBy("is_checkout")

base = (
    df_web_events
    .filter(F.col("event_time") >= F.lit("2023-09-24").cast("date"))
    .withColumn("is_checkout", F.when(F.col("page") == "/checkout", 1).otherwise(0))
)

remaining_keys = (
    base
    .select("page")
    .subtract(
        df_web_events
        .filter(~F.col("event_type").isin(["search", "purchase"]))
        .select("page")
    )
)

reconciled = (
    base
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.page") == F.col("k.page")), "inner")
    .select("s.*")
)

valid = (
    reconciled
    .filter(F.col("page").isNotNull())
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "is_checkout",
        F.lower("page").alias("page_lower"),
        F.coalesce("page", F.lit("NONE")).alias("resolved_page"),
    )
)

with_windows = (
    valid
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "page_lower",
        F.cume_dist().over(w).alias("is_checkout_cume_dist"),
    )
)

daily = (
    with_windows
    .groupBy(
        F.when(F.col("is_checkout_cume_dist") >= 0.84, "HIGH").otherwise("LOW").alias("is_checkout_cume_dist_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("page")).alias("page_list"),
        F.count("*").alias("num_rows"),
        F.round(F.sum("is_checkout_cume_dist") / F.nullif(F.count(F.when(F.col("page") == "/search", 1)), F.lit(0)), 2).alias("is_checkout_cume_dist_per_search"),
        F.percentile_approx("is_checkout_cume_dist", 0.9).alias("is_checkout_cume_dist_p90"),
        F.avg(F.when(F.col("page") == "/home", F.col("is_checkout_cume_dist"))).alias("home_avg_is_checkout_cume_dist"),
    )
)

result = (
    daily
    .select(
        "is_checkout_cume_dist_tier",
        "page_list",
        "num_rows",
        "is_checkout_cume_dist_per_search",
        "is_checkout_cume_dist_p90",
    )
    .orderBy(
        F.col("is_checkout_cume_dist_p90").asc_nulls_first(),
        F.col("is_checkout_cume_dist_tier").desc(),
    )
    .limit(10)
)
