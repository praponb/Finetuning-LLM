from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_month_site_id = Window.partitionBy(F.month("read_at"), "site_id")
w_all_by_kwh_ties = Window.orderBy(F.col("kwh").desc())
w_all_by_read_at_moving_2 = Window.orderBy("read_at", "id").rowsBetween(-2, Window.currentRow)
w_all_by_read_at = Window.orderBy("read_at", "id")
w_distinct_quality_group_in_re = Window.partitionBy("distinct_quality_group_in_read_at_month_site_id", "seq_no_filled")
w_all_by_kwh_percent_rank_ties = Window.orderBy(F.col("kwh_percent_rank").desc())
w_all_by_kwh_percent_rank_prio = Window.orderBy(F.col("kwh_percent_rank").desc(), "id").rowsBetween(-6, -1)

cleaned_readings = (
    df_meter_readings
    .filter((F.to_date(F.col("read_at")) < F.lit("2025-02-24").cast("date")) & (F.col("quality") != "OK"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        F.when(F.col("quality").isin("BAD", "EST"), "group_a").when(F.col("quality") == "OK", "group_b").otherwise("other").alias("quality_group"),
        F.concat(F.col("quality"), F.lit("#"), F.col("site_id").cast("string")).alias("quality_ref"),
    )
)

metrics = (
    cleaned_readings
    .select(
        "*",
        F.size(F.collect_set("quality_group").over(w_read_at_month_site_id)).alias("distinct_quality_group_in_read_at_month_site_id"),
        F.round(F.percent_rank().over(w_all_by_kwh_ties), 3).alias("kwh_percent_rank"),
        F.max("kwh").over(w_all_by_read_at_moving_2).alias("kwh_max_3"),
        F.row_number().over(w_all_by_read_at).alias("seq_no"),
        F.lead("kwh", 2).over(w_all_by_read_at).alias("next2_kwh"),
        F.round(F.col("kwh") - F.avg("kwh").over(w_read_at_month_site_id), 2).alias("kwh_vs_read_at_month_site_id_avg"),
        F.count(F.lit(1)).over(w_read_at_month_site_id).alias("rows_in_read_at_month_site_id"),
    )
)

derived = metrics.withColumn("seq_no_filled", F.coalesce(F.col("seq_no"), F.lit(0)))

with_windows = (
    derived
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality_group",
        "distinct_quality_group_in_read_at_month_site_id",
        "kwh_percent_rank",
        "seq_no",
        "next2_kwh",
        "kwh_vs_read_at_month_site_id_avg",
        "rows_in_read_at_month_site_id",
        "seq_no_filled",
        F.round(F.col("seq_no") - F.avg("seq_no").over(w_distinct_quality_group_in_re), 2).alias("seq_no_vs_distinct_quality_group_in_read_at_mont"),
        F.rank().over(w_all_by_kwh_percent_rank_ties).alias("kwh_percent_rank_rank"),
        F.round(F.avg("kwh").over(w_all_by_kwh_percent_rank_prio), 2).alias("kwh_avg_6_prior"),
        F.count(F.lit(1)).over(w_distinct_quality_group_in_re).alias("rows_in_distinct_quality_group_in_read_at_month_"),
        F.max("kwh").over(w_distinct_quality_group_in_re).alias("distinct_quality_group_in_read_at_month_site_id_"),
        F.round(F.col("seq_no_filled") - F.avg("seq_no_filled").over(w_distinct_quality_group_in_re), 2).alias("seq_no_filled_vs_distinct_quality_group_in_read_"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality_group",
        "distinct_quality_group_in_read_at_month_site_id",
        "kwh_percent_rank",
        "seq_no",
        "rows_in_read_at_month_site_id",
        "seq_no_filled",
        "seq_no_vs_distinct_quality_group_in_read_at_mont",
        "kwh_percent_rank_rank",
        "kwh_avg_6_prior",
        "rows_in_distinct_quality_group_in_read_at_month_",
        F.lower(F.regexp_replace(F.trim(F.col("quality_group")), " ", "_")).alias("quality_group_slug"),
    )
    .orderBy(
        F.col("site_id").desc_nulls_first(),
        "seq_no_vs_distinct_quality_group_in_read_at_mont",
    )
)
