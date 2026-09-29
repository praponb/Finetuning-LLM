from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_month_by_read_at_pri = Window.partitionBy(F.month("read_at")).orderBy(F.col("read_at").desc(), "id").rowsBetween(-6, -1)
w_read_at_month_month_quality = Window.partitionBy(F.date_trunc("month", "read_at_month"), "quality_group")
w_read_at_month_by_read_at_pri_2 = Window.partitionBy(F.month("read_at")).orderBy(F.col("read_at").desc(), "id").rowsBetween(-2, -1)
w_read_at_month_by_read_at = Window.partitionBy(F.month("read_at")).orderBy(F.col("read_at").desc(), "id")

recent = (
    df_meter_readings
    .filter((F.hour(F.col("read_at")).between(7, 11)) & (F.col("site_id") <= 4))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        F.when(F.col("quality").isin("BAD", "OK"), "group_a").when(F.col("quality") == "EST", "group_b").otherwise("other").alias("quality_group"),
        F.date_trunc("month", F.col("read_at")).alias("read_at_month"),
        F.date_format(F.col("read_at"), "yyyy-MM-dd HH:00").alias("read_at_label"),
    )
)

metrics = (
    recent
    .select(
        "*",
        F.sum("kwh").over(w_read_at_month_by_read_at_pri).alias("kwh_sum_6_prior"),
        F.size(F.collect_set("quality").over(w_read_at_month_month_quality)).alias("distinct_quality_in_read_at_month_month_quality_"),
        F.sum("kwh").over(w_read_at_month_by_read_at_pri_2).alias("kwh_sum_2_prior"),
        F.row_number().over(w_read_at_month_by_read_at).alias("read_at_seq"),
    )
)

filtered = (
    metrics
    .crossJoin(metrics.agg(F.min("kwh_sum_6_prior").alias("overall_min_kwh_sum_6_prior")))
    .filter(F.col("kwh_sum_6_prior") > F.col("overall_min_kwh_sum_6_prior"))
    .drop("overall_min_kwh_sum_6_prior")
)

with_activity = (
    filtered.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.voltage") >= 215.7) & (F.hour(F.col("m.read_at")).between(7, 12)), "left_semi")
    .filter(F.col("kwh").between(9.5, 117.4))
)

annotated = (
    with_activity
    .filter(F.col("read_at") >= F.lit("2025-01-25 11:00:00").cast("timestamp"))
    .select(
        "*",
        (F.col("kwh_sum_6_prior") + F.col("read_at_seq")).alias("kwh_sum_6_prior_plus_read_at_seq"),
        F.round(F.col("read_at_seq") / F.nullif(F.col("kwh"), F.lit(0)), 3).alias("read_at_seq_per_kwh"),
    )
)

combined_details = (
    annotated.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.quality",
        "s.quality_group",
        "s.read_at_month",
        "s.read_at_label",
        "s.kwh_sum_2_prior",
        "s.read_at_seq_per_kwh",
        F.col("si.zone").alias("site_zone"),
    )
)

quality_group_stats = (
    combined_details
    .groupBy("quality_group")
    .agg(F.percentile_approx("read_at_seq_per_kwh", 0.5).alias("median_read_at_seq_per_kwh"))
)

compared = (
    combined_details.alias("s")
    .join(quality_group_stats.alias("g"), F.col("g.quality_group") == F.col("s.quality_group"), "inner")
    .filter(F.col("s.read_at_seq_per_kwh") >= F.col("g.median_read_at_seq_per_kwh"))
    .select(
        "s.*",
        "g.median_read_at_seq_per_kwh",
        F.round(F.col("s.read_at_seq_per_kwh") - F.col("g.median_read_at_seq_per_kwh"), 2).alias("read_at_seq_per_kwh_minus_median"),
    )
)

result = (
    compared
    .filter(F.hour(F.col("read_at")).between(10, 14))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        "quality_group",
        "read_at_month",
        "kwh_sum_2_prior",
        "read_at_seq_per_kwh",
        "site_zone",
        "median_read_at_seq_per_kwh",
    )
)
