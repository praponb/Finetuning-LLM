from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_month_to_date_qualit = Window.partitionBy(F.to_date("read_at_month"), "quality_or_unknown")
w_cnt_by_kwh_to_voltage_ratio = Window.partitionBy("cnt").orderBy(F.col("kwh_to_voltage_ratio").asc_nulls_last(), "quality_or_unknown", "read_at_month")

recent = (
    df_meter_readings
    .filter((F.col("kwh") < 106.0) & (F.col("quality") == "BAD"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
    )
)

stats = (
    recent
    .groupBy(
        "quality_or_unknown",
        F.date_trunc("month", "read_at").alias("read_at_month"),
    )
    .agg(
        F.array_sort(F.collect_set("quality")).alias("quality_list"),
        F.round(F.sum("kwh") / F.nullif(F.sum("voltage"), F.lit(0)), 3).alias("kwh_to_voltage_ratio"),
        F.round(100 * F.count(F.when(~F.col("quality").isin("BAD", "EST"), 1)) / F.count("*"), 1).alias("pct_quality_not_in_bad_est"),
        F.count("*").alias("cnt"),
        F.avg("voltage").alias("avg_voltage_check"),
    )
    .filter(F.col("avg_voltage_check") > 215.7)
    .drop("avg_voltage_check")
)

ranked = (
    stats
    .select(
        "quality_or_unknown",
        "read_at_month",
        "cnt",
        F.lag("pct_quality_not_in_bad_est").over(w_cnt_by_kwh_to_voltage_ratio).alias("prev_pct_quality_not_in_bad_est"),
        F.ntile(5).over(w_cnt_by_kwh_to_voltage_ratio).alias("kwh_to_voltage_ratio_quintile"),
    )
)

result = (
    ranked
    .select(
        "quality_or_unknown",
        "read_at_month",
        "cnt",
        "prev_pct_quality_not_in_bad_est",
        "kwh_to_voltage_ratio_quintile",
        F.lower(F.regexp_replace(F.trim(F.col("quality_or_unknown")), " ", "_")).alias("quality_or_unknown_slug"),
    )
    .orderBy(
        F.col("quality_or_unknown_slug").desc(),
        "quality_or_unknown",
        "read_at_month",
    )
    .limit(5)
)
