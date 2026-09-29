from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_voltage = Window.orderBy("voltage", "id")
w_all_by_voltage_running = Window.orderBy(F.col("voltage").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_site_commissioned_on = Window.orderBy("site_commissioned_on", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_quality = Window.partitionBy("quality")
w_site_id_site_commissioned_on = Window.partitionBy("site_id", F.date_trunc("month", "site_commissioned_on"))
w_all_by_site_commissioned_on_2 = Window.orderBy("site_commissioned_on", "id")
w_site_commissioned_on_month_q = Window.partitionBy(F.date_trunc("month", "site_commissioned_on"), "quality").orderBy(F.col("site_commissioned_on").desc(), "id")
w_site_commissioned_on_month_q_2 = Window.partitionBy(F.date_trunc("month", "site_commissioned_on"), "quality").orderBy(F.col("site_commissioned_on").desc(), "id").rowsBetween(-2, -1)
w_distinct_quality_in_site_id = Window.partitionBy("distinct_quality_in_site_id_site_commissioned_on").orderBy("first_site_commissioned_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_seq_no_distinct_quality_in_s = Window.partitionBy("seq_no", "distinct_quality_in_site_id_site_commissioned_on")
w_all_by_quality_min_voltage_m = Window.orderBy(F.col("quality_min_voltage").desc(), "id").rowsBetween(-6, Window.currentRow)

prepared = (
    df_meter_readings
    .filter(F.month(F.col("read_at")).isin(1, 2))
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        F.when(F.col("quality").isin("EST", "BAD"), "group_a").when(F.col("quality") == "OK", "group_b").otherwise("other").alias("quality_group"),
    )
)

joined_site = (
    prepared.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.voltage",
        "s.quality",
        "s.quality_group",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

with_trends = (
    joined_site
    .select(
        "id",
        "voltage",
        "quality",
        "quality_group",
        F.row_number().over(w_all_by_voltage).alias("seq_no"),
        F.max("voltage").over(w_all_by_voltage_running).alias("max_voltage_so_far"),
        F.first("site_commissioned_on").over(w_all_by_site_commissioned_on).alias("first_site_commissioned_on"),
        F.min("voltage").over(w_quality).alias("quality_min_voltage"),
        F.max("voltage").over(w_site_id_site_commissioned_on).alias("site_id_site_commissioned_on_month_max_voltage"),
        F.row_number().over(w_all_by_site_commissioned_on_2).alias("site_commissioned_on_seq"),
        F.size(F.collect_set("quality").over(w_site_id_site_commissioned_on)).alias("distinct_quality_in_site_id_site_commissioned_on"),
        F.lag("site_commissioned_on").over(w_site_commissioned_on_month_q).alias("prev_site_commissioned_on"),
        F.round(F.avg("voltage").over(w_site_id_site_commissioned_on), 2).alias("site_id_site_commissioned_on_month_avg_voltage"),
        F.sum("voltage").over(w_site_commissioned_on_month_q_2).alias("voltage_sum_2_prior"),
    )
)

top_per_group = (
    with_trends
    .filter((F.col("site_commissioned_on_seq") <= 5) & (F.col("prev_site_commissioned_on") < F.lit("2025-05-07").cast("date")))
    .drop("site_commissioned_on_seq")
)

ranked = (
    top_per_group
    .select(
        "id",
        "seq_no",
        "max_voltage_so_far",
        "first_site_commissioned_on",
        "quality_min_voltage",
        "distinct_quality_in_site_id_site_commissioned_on",
        "site_id_site_commissioned_on_month_avg_voltage",
        F.first("voltage_sum_2_prior", ignorenulls=True).over(w_distinct_quality_in_site_id).alias("first_known_voltage_sum_2_prior"),
        F.count(F.lit(1)).over(w_seq_no_distinct_quality_in_s).alias("rows_in_seq_no_distinct_quality_in_site_id_site_"),
        F.round(F.col("voltage") - F.avg("voltage").over(w_seq_no_distinct_quality_in_s), 2).alias("voltage_vs_seq_no_distinct_quality_in_site_id_si"),
        F.round(F.col("quality_min_voltage") - F.avg("quality_min_voltage").over(w_seq_no_distinct_quality_in_s), 2).alias("quality_min_voltage_vs_seq_no_distinct_quality_i"),
        F.round(F.avg("voltage_sum_2_prior").over(w_all_by_quality_min_voltage_m), 2).alias("voltage_sum_2_prior_avg_7"),
    )
)

result = (
    ranked
    .select(
        "id",
        "max_voltage_so_far",
        "first_site_commissioned_on",
        "quality_min_voltage",
        "distinct_quality_in_site_id_site_commissioned_on",
        "first_known_voltage_sum_2_prior",
        "rows_in_seq_no_distinct_quality_in_site_id_site_",
        "voltage_vs_seq_no_distinct_quality_in_site_id_si",
        "quality_min_voltage_vs_seq_no_distinct_quality_i",
        "voltage_sum_2_prior_avg_7",
    )
    .orderBy(F.col("id").asc())
    .limit(10)
)
