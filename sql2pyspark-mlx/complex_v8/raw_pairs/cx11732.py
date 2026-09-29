from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_n_quality_ok_quality_by_n_qu = Window.partitionBy("n_quality_ok", "quality").orderBy("n_quality_ok_distance", "read_at_month", "quality").rowsBetween(-6, -1)
w_n_quality_ok_distance_by_vol = Window.partitionBy("n_quality_ok_distance").orderBy("voltage_to_quality_len_ratio", "read_at_month", "quality")
w_cnt_tier_n_quality_ok_distan = Window.partitionBy("cnt_tier", "n_quality_ok_distance")

valid = (
    df_meter_readings
    .filter(F.col("quality").isNotNull())
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

enriched = (
    valid
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        F.length(F.trim(F.col("quality"))).alias("quality_len"),
        (F.col("read_at") + F.expr("INTERVAL 30 MINUTES")).alias("read_at_plus_30m"),
        F.when(F.col("voltage") >= 233.8, "large").when(F.col("voltage") >= 216.8, "medium").otherwise("small").alias("voltage_tier"),
    )
)

grouped = (
    enriched
    .filter(F.col("read_at_plus_30m") >= F.lit("2025-01-25 09:00:00").cast("timestamp"))
    .groupBy(F.date_trunc("month", "read_at").alias("read_at_month"), "quality")
    .agg(
        F.round(100 * F.count(F.when(~F.col("quality").isin("BAD", "EST"), 1)) / F.count("*"), 1).alias("pct_quality_not_in_bad_est"),
        F.round(F.sum("voltage") / F.nullif(F.sum("quality_len"), F.lit(0)), 3).alias("voltage_to_quality_len_ratio"),
        F.count(F.when(F.col("quality") == "OK", 1)).alias("n_quality_ok"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("cnt") >= 3)
)

scored = (
    grouped
    .filter(F.col("pct_quality_not_in_bad_est") > 25.0)
    .select("*", F.abs(F.col("n_quality_ok") - 6).alias("n_quality_ok_distance"))
)

tagged = (
    scored
    .select(
        "read_at_month",
        "quality",
        "voltage_to_quality_len_ratio",
        "n_quality_ok",
        "n_quality_ok_distance",
        F.when(F.col("cnt") >= 8, "large").when(F.col("cnt") >= 6, "medium").otherwise("small").alias("cnt_tier"),
    )
)

filtered = (
    tagged
    .crossJoin(tagged.agg(F.avg("n_quality_ok_distance").alias("overall_avg_n_quality_ok_distance")))
    .filter(F.col("n_quality_ok_distance") > F.col("overall_avg_n_quality_ok_distance"))
    .drop("overall_avg_n_quality_ok_distance")
)

windowed = (
    filtered
    .select(
        "*",
        F.round(F.avg("n_quality_ok_distance").over(w_n_quality_ok_quality_by_n_qu), 2).alias("n_quality_ok_distance_avg_6_prior"),
        F.round((F.unix_timestamp("read_at_month") - F.unix_timestamp(F.lag("read_at_month").over(w_n_quality_ok_distance_by_vol))) / 60, 1).alias("minutes_since_prev_read_at_month"),
        F.round(F.col("voltage_to_quality_len_ratio") / F.nullif(F.sum("voltage_to_quality_len_ratio").over(w_cnt_tier_n_quality_ok_distan), F.lit(0)), 4).alias("voltage_to_quality_len_ratio_share_of_cnt_tier_n"),
        F.lead("voltage_to_quality_len_ratio", 3, 0).over(w_n_quality_ok_distance_by_vol).alias("next3_voltage_to_quality_len_ratio"),
    )
)

result = (
    windowed
    .select(
        "read_at_month",
        "n_quality_ok",
        "n_quality_ok_distance",
        "n_quality_ok_distance_avg_6_prior",
        "minutes_since_prev_read_at_month",
        "next3_voltage_to_quality_len_ratio",
        F.lower(F.trim(F.col("cnt_tier"))).alias("cnt_tier_clean"),
        F.least(F.col("n_quality_ok_distance"), F.col("n_quality_ok")).alias("least_n_quality_ok_distance_n_quality_ok"),
    )
    .orderBy(
        F.col("cnt_tier_clean").desc(),
        F.col("next3_voltage_to_quality_len_ratio").asc_nulls_last(),
    )
)
