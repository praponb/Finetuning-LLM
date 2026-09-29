from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_reading_id_site_id = Window.partitionBy("reading_id", "site_id")
w_reading_id_reading_quality_b = Window.partitionBy("reading_id", "reading_quality").orderBy("reading_kwh", "id", "reading_id")
w_reading_id_reading_quality_b_2 = Window.partitionBy("reading_id", "reading_quality").orderBy("reading_kwh", "id", "reading_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_reading_id_reading_quality_b_3 = Window.partitionBy("reading_id", "reading_quality").orderBy("reading_kwh", "id", "reading_id").rowsBetween(-3, -1)
w_distinct_reading_quality_in = Window.partitionBy("distinct_reading_quality_in_reading_id_site_id")
w_n_reading_quality_is_not_nul = Window.partitionBy("n_reading_quality_is_not_null", "reading_kwh_seq").orderBy(F.col("min_reading_kwh_seq").desc())
w_n_reading_quality_is_not_nul_2 = Window.partitionBy("n_reading_quality_is_not_null", "reading_kwh_seq").orderBy(F.col("min_reading_kwh_seq").desc(), "reading_kwh_seq", "distinct_reading_quality_in_reading_id_site_id")
w_all_by_n_reading_quality_is = Window.orderBy("n_reading_quality_is_not_null", "reading_kwh_seq", "distinct_reading_quality_in_reading_id_site_id")
w_min_reading_kwh_seq_reading = Window.partitionBy("min_reading_kwh_seq", "reading_kwh_seq").orderBy("max_reading_voltage_avg_3_prior")

prepared_readings = (
    df_meter_readings
    .filter((F.month(F.col("read_at")).isin(2, 3)) & (F.col("kwh") <= 109.0))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
    )
)

line_level = (
    prepared_readings.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        F.col("m.id").alias("reading_id"),
        F.col("m.kwh").alias("reading_kwh"),
        F.col("m.quality").alias("reading_quality"),
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.voltage").alias("reading_voltage"),
    )
)

with_windows = (
    line_level
    .select(
        "*",
        F.max("reading_kwh").over(w_reading_id_site_id).alias("reading_id_site_id_max_reading_kwh"),
        F.size(F.collect_set("reading_quality").over(w_reading_id_site_id)).alias("distinct_reading_quality_in_reading_id_site_id"),
        F.row_number().over(w_reading_id_reading_quality_b).alias("reading_kwh_seq"),
        F.min("reading_voltage").over(w_reading_id_site_id).alias("reading_id_site_id_min_reading_voltage"),
        F.nth_value("reading_voltage", 2).over(w_reading_id_reading_quality_b_2).alias("second_reading_voltage"),
        F.round(F.avg("reading_voltage").over(w_reading_id_reading_quality_b_3), 2).alias("reading_voltage_avg_3_prior"),
    )
)

stats = (
    with_windows
    .groupBy("reading_kwh_seq", "distinct_reading_quality_in_reading_id_site_id")
    .agg(
        F.count(F.when(F.col("reading_quality").isNotNull(), 1)).alias("n_reading_quality_is_not_null"),
        F.max("reading_voltage_avg_3_prior").alias("max_reading_voltage_avg_3_prior"),
        F.max("reading_kwh").alias("max_reading_kwh"),
        F.countDistinct("id").alias("distinct_ids"),
        F.min("reading_kwh_seq").alias("min_reading_kwh_seq"),
        F.avg("distinct_reading_quality_in_reading_id_site_id").alias("avg_distinct_reading_quality_in_reading_id_site_"),
    )
    .filter(F.col("avg_distinct_reading_quality_in_reading_id_site_") > 2)
    .drop("avg_distinct_reading_quality_in_reading_id_site_")
)

qualified = (
    stats
    .crossJoin(stats.agg(F.max("n_reading_quality_is_not_null").alias("overall_max_n_reading_quality_is_not_null")))
    .filter(F.col("n_reading_quality_is_not_null") < F.col("overall_max_n_reading_quality_is_not_null"))
    .drop("overall_max_n_reading_quality_is_not_null")
)

windowed = (
    qualified
    .select(
        "*",
        F.round(F.col("max_reading_kwh") / F.nullif(F.sum("max_reading_kwh").over(w_distinct_reading_quality_in), F.lit(0)), 4).alias("max_reading_kwh_share_of_distinct_reading_qualit"),
        F.round(F.cume_dist().over(w_n_reading_quality_is_not_nul), 3).alias("min_reading_kwh_seq_cume_dist"),
        F.count(F.lit(1)).over(w_distinct_reading_quality_in).alias("rows_in_distinct_reading_quality_in_reading_id_s"),
        F.lag("max_reading_kwh").over(w_n_reading_quality_is_not_nul_2).alias("prev_max_reading_kwh"),
        F.ntile(10).over(w_all_by_n_reading_quality_is).alias("n_reading_quality_is_not_null_decile"),
        F.dense_rank().over(w_min_reading_kwh_seq_reading).alias("max_reading_voltage_avg_3_prior_dense_rank"),
    )
)

result = (
    windowed
    .select(
        "reading_kwh_seq",
        "distinct_reading_quality_in_reading_id_site_id",
        "n_reading_quality_is_not_null",
        "max_reading_kwh",
        "distinct_ids",
        "min_reading_kwh_seq",
        "max_reading_kwh_share_of_distinct_reading_qualit",
        "prev_max_reading_kwh",
        "n_reading_quality_is_not_null_decile",
        "max_reading_voltage_avg_3_prior_dense_rank",
    )
    .orderBy(
        F.col("reading_kwh_seq").desc_nulls_first(),
        F.col("prev_max_reading_kwh").desc_nulls_first(),
    )
)
