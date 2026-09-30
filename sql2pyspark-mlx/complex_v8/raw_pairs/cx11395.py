from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_read_at = Window.orderBy("read_at", "id")
w_site_id_quality = Window.partitionBy("site_id", "quality")
w_site_id_quality_or_unknown_b = Window.partitionBy("site_id", "quality_or_unknown").orderBy(F.col("kwh_minus_voltage").desc(), "id").rowsBetween(-3, -1)
w_all_by_kwh_minus_voltage_tie = Window.orderBy(F.col("kwh_minus_voltage").asc_nulls_last())
w_all_by_kwh_minus_voltage_mov = Window.orderBy(F.col("kwh_minus_voltage").asc_nulls_last(), "id").rowsBetween(-1, Window.currentRow)
w_site_id_quality_or_unknown_b_2 = Window.partitionBy("site_id", "quality_or_unknown").orderBy(F.col("kwh_minus_voltage").desc(), "id")
w_site_id_quality_or_unknown_b_3 = Window.partitionBy("site_id", "quality_or_unknown").orderBy("voltage", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_quality_or_unknown_quality = Window.partitionBy("quality_or_unknown", "quality")
w_site_id_quality_or_unknown_b_4 = Window.partitionBy("site_id", "quality_or_unknown").orderBy(F.col("kwh_minus_voltage").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent = (
    df_meter_readings
    .filter(F.col("quality") == "EST")
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        (F.col("kwh") - F.col("voltage")).alias("kwh_minus_voltage"),
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
    )
)

windowed = (
    recent
    .select(
        "*",
        F.ntile(3).over(w_all_by_read_at).alias("read_at_tercile"),
        F.sum("kwh").over(w_site_id_quality).alias("site_id_quality_sum_kwh"),
        F.round(F.col("kwh_minus_voltage") - F.avg("kwh_minus_voltage").over(w_site_id_quality), 2).alias("kwh_minus_voltage_vs_site_id_quality_avg"),
        F.max("kwh_minus_voltage").over(w_site_id_quality_or_unknown_b).alias("kwh_minus_voltage_max_3_prior"),
        F.round(F.cume_dist().over(w_all_by_kwh_minus_voltage_tie), 3).alias("kwh_minus_voltage_cume_dist"),
        F.size(F.collect_set("quality_or_unknown").over(w_site_id_quality)).alias("distinct_quality_or_unknown_in_site_id_quality"),
        F.lead("read_at", 2).over(w_all_by_read_at).alias("next2_read_at"),
        F.max("kwh_minus_voltage").over(w_all_by_kwh_minus_voltage_mov).alias("kwh_minus_voltage_max_2"),
        F.lead("kwh", 3).over(w_site_id_quality_or_unknown_b_2).alias("next3_kwh"),
        F.first("read_at", ignorenulls=True).over(w_site_id_quality_or_unknown_b_3).alias("first_known_read_at"),
        F.max("kwh").over(w_quality_or_unknown_quality).alias("quality_or_unknown_quality_max_kwh"),
        F.count(F.lit(1)).over(w_quality_or_unknown_quality).alias("rows_in_quality_or_unknown_quality"),
        F.first("voltage", ignorenulls=True).over(w_site_id_quality_or_unknown_b_4).alias("first_known_voltage"),
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
        "kwh_minus_voltage",
        "quality_or_unknown",
        "read_at_tercile",
        "site_id_quality_sum_kwh",
        "kwh_minus_voltage_vs_site_id_quality_avg",
        "kwh_minus_voltage_max_3_prior",
        "kwh_minus_voltage_cume_dist",
        "distinct_quality_or_unknown_in_site_id_quality",
        "next2_read_at",
        "kwh_minus_voltage_max_2",
        "first_known_read_at",
        "rows_in_quality_or_unknown_quality",
        "first_known_voltage",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("next2_read_at")).alias("days_since_next2_read_at"),
        F.when(F.col("quality").isin("EST", "BAD"), "group_a").when(F.col("quality") == "OK", "group_b").otherwise("other").alias("quality_group"),
    )
)

result = (
    tagged
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        "kwh_minus_voltage",
        "read_at_tercile",
        "site_id_quality_sum_kwh",
        "kwh_minus_voltage_vs_site_id_quality_avg",
        "kwh_minus_voltage_max_3_prior",
        "kwh_minus_voltage_cume_dist",
        "first_known_read_at",
        "rows_in_quality_or_unknown_quality",
        "first_known_voltage",
        "quality_group",
        F.initcap(F.trim(F.col("quality_or_unknown"))).alias("quality_or_unknown_clean"),
    )
    .orderBy(F.col("quality_group").desc())
)
