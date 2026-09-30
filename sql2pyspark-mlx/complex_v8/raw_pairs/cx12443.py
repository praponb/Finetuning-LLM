from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_to_date_site_id = Window.partitionBy(F.to_date("read_at"), "site_id")
w_quality_by_kwh_adj = Window.partitionBy("quality").orderBy(F.col("kwh_adj").desc(), "id")
w_all_by_days_since_read_at = Window.orderBy("days_since_read_at", "id")
w_quality_site_id = Window.partitionBy("quality", "site_id")
w_quality_by_kwh_adj_running = Window.partitionBy("quality").orderBy(F.col("kwh_adj").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_read_at_year_by_kwh_moving_1 = Window.partitionBy(F.year("read_at")).orderBy(F.col("kwh").asc_nulls_last(), "id").rowsBetween(-1, Window.currentRow)
w_read_at_year_by_kwh = Window.partitionBy(F.year("read_at")).orderBy(F.col("kwh").asc_nulls_last(), "id")
w_site_id_prev_read_at_month = Window.partitionBy("site_id", F.date_trunc("month", "prev_read_at"))
w_site_id_by_days_since_read_a = Window.partitionBy("site_id").orderBy(F.col("days_since_read_at_seq").asc_nulls_last(), "id").rowsBetween(-1, Window.currentRow)
w_prev_read_at_to_date_site_id = Window.partitionBy(F.to_date("prev_read_at"), "site_id").orderBy(F.col("days_since_read_at_seq").desc(), "id")

filtered = (
    df_meter_readings
    .filter(F.col("voltage") >= 215.6)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.round(F.col("kwh") * 0.9, 2).alias("kwh_adj"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("read_at")).alias("days_since_read_at"),
    )
)

joined_site = (
    filtered.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.site_name").isNotNull())
    .select("s.*", "si.site_name")
)

ranked = (
    joined_site
    .select(
        "id",
        "site_id",
        "voltage",
        "quality",
        "kwh_adj",
        "days_since_read_at",
        "site_name",
        F.round(F.col("days_since_read_at") - F.avg("days_since_read_at").over(w_read_at_to_date_site_id), 2).alias("days_since_read_at_vs_read_at_to_date_site_id_av"),
        F.lag("read_at").over(w_quality_by_kwh_adj).alias("prev_read_at"),
        F.row_number().over(w_all_by_days_since_read_at).alias("days_since_read_at_seq"),
        F.sum("days_since_read_at").over(w_quality_by_kwh_adj_running).alias("running_days_since_read_at"),
        F.min("kwh").over(w_quality_site_id).alias("quality_site_id_min_kwh"),
        F.max("days_since_read_at").over(w_read_at_year_by_kwh_moving_1).alias("days_since_read_at_max_2"),
        F.lead("days_since_read_at", 1, 0).over(w_quality_by_kwh_adj).alias("next_days_since_read_at"),
        F.round(F.cume_dist().over(w_read_at_year_by_kwh), 3).alias("kwh_cume_dist"),
        F.size(F.collect_set("quality").over(w_read_at_to_date_site_id)).alias("distinct_quality_in_read_at_to_date_site_id"),
    )
)

tagged = (
    ranked
    .withColumn("days_since_read_at_per_voltage", F.round(F.col("days_since_read_at") / F.nullif(F.col("voltage"), F.lit(0)), 3))
)

metrics = (
    tagged
    .select(
        "id",
        "site_id",
        "quality",
        "kwh_adj",
        "days_since_read_at",
        "site_name",
        "days_since_read_at_vs_read_at_to_date_site_id_av",
        "prev_read_at",
        "days_since_read_at_seq",
        "quality_site_id_min_kwh",
        "days_since_read_at_max_2",
        "kwh_cume_dist",
        F.count(F.lit(1)).over(w_site_id_prev_read_at_month).alias("rows_in_site_id_prev_read_at_month"),
        F.size(F.collect_set("quality").over(w_site_id_prev_read_at_month)).alias("distinct_quality_in_site_id_prev_read_at_month"),
        F.max("days_since_read_at").over(w_site_id_by_days_since_read_a).alias("days_since_read_at_max_2_2"),
        F.round(F.cume_dist().over(w_prev_read_at_to_date_site_id), 3).alias("days_since_read_at_seq_cume_dist"),
        F.round(F.col("kwh_cume_dist") / F.nullif(F.sum("kwh_cume_dist").over(w_site_id_prev_read_at_month), F.lit(0)), 4).alias("kwh_cume_dist_share_of_site_id_prev_read_at_mont"),
    )
)

result = (
    metrics
    .filter(F.col("days_since_read_at") > 140)
    .select(
        "id",
        "site_id",
        "quality",
        "kwh_adj",
        "days_since_read_at",
        "site_name",
        "days_since_read_at_vs_read_at_to_date_site_id_av",
        "prev_read_at",
        "days_since_read_at_seq",
        "quality_site_id_min_kwh",
        "days_since_read_at_max_2",
        "kwh_cume_dist",
        "rows_in_site_id_prev_read_at_month",
        "distinct_quality_in_site_id_prev_read_at_month",
        "days_since_read_at_seq_cume_dist",
        "kwh_cume_dist_share_of_site_id_prev_read_at_mont",
    )
    .orderBy(
        F.col("days_since_read_at_vs_read_at_to_date_site_id_av").desc(),
        F.col("prev_read_at").desc(),
    )
)
