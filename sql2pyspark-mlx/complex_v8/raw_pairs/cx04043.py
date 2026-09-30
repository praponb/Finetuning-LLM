from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id")
w_site_id_site_commissioned_on = Window.partitionBy("site_id", F.date_trunc("month", "site_commissioned_on")).orderBy(F.col("voltage").desc(), "id")
w_site_id_by_site_commissioned = Window.partitionBy("site_id").orderBy(F.col("site_commissioned_on").desc(), "id").rowsBetween(-2, -1)
w_site_id_by_site_commissioned_2 = Window.partitionBy("site_id").orderBy(F.col("site_commissioned_on").desc(), "id")
w_quality_site_id = Window.partitionBy("quality", "site_id")
w_all_by_voltage = Window.orderBy(F.col("voltage").desc(), "id")

base = (
    df_meter_readings
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
    base.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.quality",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        "si.site_name",
    )
)

windowed = (
    enriched
    .select(
        "*",
        F.count(F.lit(1)).over(w_site_id).alias("rows_in_site_id"),
        F.lag("site_commissioned_on").over(w_site_id_site_commissioned_on).alias("prev_site_commissioned_on"),
        F.sum("voltage").over(w_site_id).alias("site_id_sum_voltage"),
        F.row_number().over(w_site_id_site_commissioned_on).alias("rn"),
        F.max("kwh").over(w_site_id_by_site_commissioned).alias("kwh_max_2_prior"),
        F.lead("site_commissioned_on", 2).over(w_site_id_by_site_commissioned_2).alias("next2_site_commissioned_on"),
        (F.col("voltage") - F.lag("voltage").over(w_site_id_by_site_commissioned_2)).alias("voltage_change"),
        F.row_number().over(w_site_id_by_site_commissioned_2).alias("rn_2"),
        F.round(F.col("voltage") / F.nullif(F.sum("voltage").over(w_site_id), F.lit(0)), 4).alias("voltage_share_of_site_id"),
        F.round(F.avg("voltage").over(w_site_id), 2).alias("site_id_avg_voltage"),
        F.lead("voltage").over(w_all_by_voltage).alias("next_voltage"),
        F.round(F.col("voltage") - F.avg("voltage").over(w_site_id), 2).alias("voltage_vs_site_id_avg"),
    )
)

tagged = (
    windowed
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        "site_commissioned_on",
        "site_name",
        "prev_site_commissioned_on",
        "site_id_sum_voltage",
        "kwh_max_2_prior",
        "next2_site_commissioned_on",
        "voltage_change",
        "voltage_share_of_site_id",
        "site_id_avg_voltage",
        "voltage_vs_site_id_avg",
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
        F.when(F.col("quality").isin("BAD", "EST"), "group_a").when(F.col("quality") == "OK", "group_b").otherwise("other").alias("quality_group"),
        F.date_format(F.col("site_commissioned_on"), "yyyy-MM").alias("site_commissioned_on_label"),
    )
)

rollup_base = (
    tagged
    .groupBy(
        F.date_trunc("month", "site_commissioned_on").alias("site_commissioned_on_month"),
        "site_id",
        "quality",
    )
    .agg(
        F.round(F.sum("site_id_sum_voltage") / F.nullif(F.sum("kwh"), F.lit(0)), 3).alias("site_id_sum_voltage_to_kwh_ratio"),
        F.min("site_commissioned_on").alias("first_site_commissioned_on"),
        F.round(100 * F.count(F.when(F.lower(F.col("site_name")).like("% b%"), 1)) / F.count("*"), 1).alias("pct_lower_site_name_like_b"),
        F.sum("site_id_sum_voltage").alias("total_site_id_sum_voltage"),
        F.sum("voltage_share_of_site_id").alias("sum_voltage_share_of_site_id_check"),
    )
    .filter(F.col("sum_voltage_share_of_site_id_check") > 0.1)
    .drop("sum_voltage_share_of_site_id_check")
)

result = (
    rollup_base
    .filter(~F.col("quality").isin("EST", "OK"))
    .select(
        "site_commissioned_on_month",
        "site_id",
        "quality",
        "site_id_sum_voltage_to_kwh_ratio",
        "total_site_id_sum_voltage",
        F.upper(F.trim(F.col("quality"))).alias("quality_clean"),
    )
    .orderBy(F.col("quality").desc_nulls_first())
)
