from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_site_id_by_read_at_r = Window.partitionBy("quality", "site_id").orderBy(F.col("read_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_quality_site_id_by_voltage_t = Window.partitionBy("quality", "site_id").orderBy("voltage")
w_quality_site_id_by_read_at_t = Window.partitionBy("quality", "site_id").orderBy(F.col("read_at").desc())
w_quality_site_id_by_read_at = Window.partitionBy("quality", "site_id").orderBy(F.col("read_at").desc(), "id")
w_read_at_month = Window.partitionBy(F.date_trunc("month", "read_at"))
w_site_id_quality_by_read_at = Window.partitionBy("site_id", "quality").orderBy(F.col("read_at").desc(), "id")
w_site_id_quality_by_read_at_p = Window.partitionBy("site_id", "quality").orderBy(F.col("read_at").desc(), "id").rowsBetween(-3, -1)
w_site_id_quality_by_read_at_r = Window.partitionBy("site_id", "quality").orderBy(F.col("read_at_rank").desc(), "id").rowsBetween(-2, -1)
w_read_at_rank_quality = Window.partitionBy("read_at_rank", "quality")
w_read_at_year_quality_by_read = Window.partitionBy(F.year("read_at"), "quality").orderBy("read_at", "id")
w_read_at_year_quality_by_read_2 = Window.partitionBy(F.year("read_at"), "quality").orderBy("read_at")
w_all_by_read_at_rank_quality = Window.orderBy(F.col("read_at_rank_quality_avg_rows_in_read_at_month_t").desc())
w_distinct_site_ids_seq_no = Window.partitionBy("distinct_site_ids", "seq_no")
w_all_by_read_at_rank_quality_2 = Window.orderBy(F.col("read_at_rank_quality_avg_rows_in_read_at_month_t").desc(), "seq_no")

base = (
    df_meter_readings
    .filter(~F.col("quality").isin("EST", "BAD"))
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
    )
)

windowed = (
    base
    .select(
        "*",
        F.sum("voltage").over(w_quality_site_id_by_read_at_r).alias("running_voltage"),
        F.rank().over(w_quality_site_id_by_voltage_t).alias("voltage_rank"),
        F.round(F.cume_dist().over(w_quality_site_id_by_read_at_t), 3).alias("read_at_cume_dist"),
        F.rank().over(w_quality_site_id_by_read_at).alias("read_at_rank"),
        F.count(F.lit(1)).over(w_read_at_month).alias("rows_in_read_at_month"),
        F.lag("voltage").over(w_site_id_quality_by_read_at).alias("prev_voltage"),
        F.round(F.avg("voltage").over(w_site_id_quality_by_read_at_p), 2).alias("voltage_avg_3_prior"),
        F.round(F.percent_rank().over(w_quality_site_id_by_read_at_t), 3).alias("read_at_percent_rank"),
        F.min("voltage").over(w_read_at_month).alias("read_at_month_min_voltage"),
        F.max("voltage").over(w_read_at_month).alias("read_at_month_max_voltage"),
    )
)

enriched = (
    windowed
    .filter(F.col("rows_in_read_at_month").between(4, 19))
    .withColumn("voltage_avg_3_prior_filled", F.coalesce(F.col("voltage_avg_3_prior"), F.lit(0.0)))
)

metrics = (
    enriched
    .select(
        "*",
        F.round(F.avg("running_voltage").over(w_site_id_quality_by_read_at_r), 2).alias("running_voltage_avg_2_prior"),
        F.count(F.lit(1)).over(w_read_at_rank_quality).alias("rows_in_read_at_rank_quality"),
        F.row_number().over(w_read_at_year_quality_by_read).alias("seq_no"),
        F.round(F.cume_dist().over(w_read_at_year_quality_by_read_2), 3).alias("read_at_cume_dist_2"),
        F.round(F.avg("rows_in_read_at_month").over(w_read_at_rank_quality), 2).alias("read_at_rank_quality_avg_rows_in_read_at_month"),
    )
)

grouped = (
    metrics
    .groupBy("seq_no")
    .agg(
        F.round(F.sum("read_at_rank_quality_avg_rows_in_read_at_month") / F.nullif(F.sum("rows_in_read_at_month"), F.lit(0)), 3).alias("read_at_rank_quality_avg_rows_in_read_at_month_t"),
        F.countDistinct("quality").alias("distinct_qualitys"),
        F.round(F.sum("voltage_avg_3_prior") / F.nullif(F.sum("read_at_rank_quality_avg_rows_in_read_at_month"), F.lit(0)), 3).alias("voltage_avg_3_prior_to_read_at_rank_quality_avg_"),
        F.countDistinct("site_id").alias("distinct_site_ids"),
    )
)

with_windows = (
    grouped
    .select(
        "*",
        F.round(F.percent_rank().over(w_all_by_read_at_rank_quality), 3).alias("read_at_rank_quality_avg_rows_in_read_at_month_t_2"),
        F.round(F.col("distinct_site_ids") - F.avg("distinct_site_ids").over(w_distinct_site_ids_seq_no), 2).alias("distinct_site_ids_vs_distinct_site_ids_seq_no_av"),
        F.ntile(5).over(w_all_by_read_at_rank_quality_2).alias("read_at_rank_quality_avg_rows_in_read_at_month_t_3"),
    )
)

result = (
    with_windows
    .select(
        "seq_no",
        "read_at_rank_quality_avg_rows_in_read_at_month_t",
        "read_at_rank_quality_avg_rows_in_read_at_month_t_2",
        "read_at_rank_quality_avg_rows_in_read_at_month_t_3",
        F.least(F.col("seq_no"), F.col("distinct_site_ids")).alias("least_seq_no_distinct_site_ids"),
        F.abs(F.col("seq_no") - 5).alias("seq_no_distance"),
    )
)
