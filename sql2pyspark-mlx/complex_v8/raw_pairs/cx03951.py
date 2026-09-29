from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality = Window.partitionBy("quality")
w_all_by_max_kwh_per_voltage_f = Window.orderBy("max_kwh_per_voltage", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_read_at_month = Window.partitionBy(F.month("read_at"))
w_all_by_kwh_per_voltage_minus = Window.orderBy("kwh_per_voltage_minus_max", "id")

filtered_readings = (
    df_meter_readings
    .filter(F.col("voltage") < 233.8)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        F.round(F.col("kwh") / F.nullif(F.col("voltage"), F.lit(0)), 3).alias("kwh_per_voltage"),
        F.when(F.col("voltage") >= 237.8, "A").when(F.col("voltage") >= 221.4, "B").otherwise("C").alias("voltage_tier"),
        F.round(F.col("kwh") * 1.07, 2).alias("kwh_adj"),
    )
)

site_id_stats = (
    filtered_readings
    .groupBy("site_id")
    .agg(F.max("kwh_per_voltage").alias("max_kwh_per_voltage"))
)

compared = (
    filtered_readings.alias("s")
    .join(site_id_stats.alias("g"), F.col("g.site_id") == F.col("s.site_id"), "inner")
    .filter(F.col("s.kwh_per_voltage") == F.col("g.max_kwh_per_voltage"))
    .select(
        "s.*",
        "g.max_kwh_per_voltage",
        F.round(F.col("s.kwh_per_voltage") - F.col("g.max_kwh_per_voltage"), 2).alias("kwh_per_voltage_minus_max"),
    )
)

windowed = (
    compared
    .select(
        "*",
        F.round(F.col("kwh_per_voltage_minus_max") / F.nullif(F.sum("kwh_per_voltage_minus_max").over(w_quality), F.lit(0)), 4).alias("kwh_per_voltage_minus_max_share_of_quality"),
        F.first("kwh").over(w_all_by_max_kwh_per_voltage_f).alias("first_kwh"),
        F.size(F.collect_set("voltage_tier").over(w_quality)).alias("distinct_voltage_tier_in_quality"),
    )
)

with_trends = (
    windowed
    .select(
        "*",
        F.round(F.col("kwh") / F.nullif(F.sum("kwh").over(w_read_at_month), F.lit(0)), 4).alias("kwh_share_of_read_at_month"),
        (F.col("kwh_per_voltage") - F.lag("kwh_per_voltage").over(w_all_by_kwh_per_voltage_minus)).alias("kwh_per_voltage_change"),
    )
)

result = (
    with_trends
    .cube("quality", "distinct_voltage_tier_in_quality", "voltage_tier")
    .agg(
        F.array_sort(F.collect_set("quality")).alias("quality_list"),
        F.count(F.when(F.col("quality") == "OK", 1)).alias("n_quality_ok"),
    )
    .orderBy(
        F.col("quality").asc_nulls_last(),
        F.col("distinct_voltage_tier_in_quality").asc_nulls_last(),
        F.col("voltage_tier").asc_nulls_last(),
    )
)
