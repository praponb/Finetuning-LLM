from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_day_year_n_quality_i = Window.partitionBy(F.year("read_at_day"), "n_quality_is_not_null").orderBy("min_max_kwh", "read_at_day")
w_n_quality_is_not_null_read_a = Window.partitionBy("n_quality_is_not_null", F.year("read_at_day"))

base = (
    df_meter_readings
    .filter(F.col("voltage") > 222.3)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        F.round(F.col("kwh") / F.nullif(F.col("voltage"), F.lit(0)), 3).alias("kwh_per_voltage"),
        F.round(F.col("voltage") / F.nullif(F.col("kwh"), F.lit(0)), 3).alias("voltage_per_kwh"),
    )
)

site_id_stats = (
    base
    .groupBy("site_id")
    .agg(F.max("kwh").alias("max_kwh"))
)

vs_group = (
    base.alias("s")
    .join(site_id_stats.alias("g"), F.col("g.site_id") == F.col("s.site_id"), "inner")
    .filter(F.col("s.kwh") == F.col("g.max_kwh"))
    .select(
        "s.*",
        "g.max_kwh",
        F.round(F.col("s.kwh") - F.col("g.max_kwh"), 2).alias("kwh_minus_max"),
    )
)

rollup_base = (
    vs_group
    .groupBy(F.to_date("read_at").alias("read_at_day"))
    .agg(
        F.count(F.when(F.col("quality").isNotNull(), 1)).alias("n_quality_is_not_null"),
        F.min("max_kwh").alias("min_max_kwh"),
        F.round(F.sum("voltage_per_kwh"), 2).alias("total_voltage_per_kwh"),
        F.max("voltage_per_kwh").alias("max_voltage_per_kwh_check"),
    )
    .filter(F.col("max_voltage_per_kwh_check") > 2.5)
    .drop("max_voltage_per_kwh_check")
)

with_windows = (
    rollup_base
    .select(
        "*",
        F.lag("n_quality_is_not_null").over(w_read_at_day_year_n_quality_i).alias("prev_n_quality_is_not_null"),
        F.round(F.col("min_max_kwh") / F.nullif(F.sum("min_max_kwh").over(w_n_quality_is_not_null_read_a), F.lit(0)), 4).alias("min_max_kwh_share_of_n_quality_is_not_null_read_"),
        F.sum("n_quality_is_not_null").over(w_n_quality_is_not_null_read_a).alias("n_quality_is_not_null_read_at_day_year_sum_n_qua"),
        F.round(F.col("n_quality_is_not_null") / F.nullif(F.sum("n_quality_is_not_null").over(w_n_quality_is_not_null_read_a), F.lit(0)), 4).alias("n_quality_is_not_null_share_of_n_quality_is_not_"),
    )
)

result = (
    with_windows
    .filter(F.col("prev_n_quality_is_not_null") < 8)
    .select(
        "read_at_day",
        "n_quality_is_not_null",
        "total_voltage_per_kwh",
        "prev_n_quality_is_not_null",
        "min_max_kwh_share_of_n_quality_is_not_null_read_",
        "n_quality_is_not_null_read_at_day_year_sum_n_qua",
        "n_quality_is_not_null_share_of_n_quality_is_not_",
        (F.col("min_max_kwh") - F.col("total_voltage_per_kwh")).alias("min_max_kwh_minus_total_voltage_per_kwh"),
    )
    .orderBy(F.col("min_max_kwh_minus_total_voltage_per_kwh").desc())
)
