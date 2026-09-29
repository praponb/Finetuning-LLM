from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id")
w_site_id_by_read_at_full = Window.partitionBy("site_id").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_voltage_prior_1 = Window.orderBy(F.col("voltage").asc_nulls_last(), "id").rowsBetween(-1, -1)
w_site_id_read_at_to_date = Window.partitionBy("site_id", F.to_date("read_at"))
w_site_id_by_read_at_ties = Window.partitionBy("site_id").orderBy("read_at")
w_site_id_by_read_at = Window.partitionBy("site_id").orderBy("read_at", "id")
w_quality_site_id = Window.partitionBy("quality", "site_id")
w_all_by_voltage = Window.orderBy(F.col("voltage").asc_nulls_last(), "id")
w_read_at_to_date_quality_by_k = Window.partitionBy(F.to_date("read_at"), "quality").orderBy("kwh", "id")
w_all_by_voltage_ties = Window.orderBy(F.col("voltage").asc_nulls_last())
w_all_by_voltage_running = Window.orderBy(F.col("voltage").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_id_read_at_rank_by_seco = Window.partitionBy("site_id", "read_at_rank").orderBy("second_kwh", "id")
w_distinct_quality_in_site_id = Window.partitionBy("distinct_quality_in_site_id").orderBy(F.col("voltage_share_of_quality_site_id").desc(), "id")
w_distinct_quality_in_site_id_2 = Window.partitionBy("distinct_quality_in_site_id").orderBy(F.col("voltage_share_of_quality_site_id").desc(), "id").rowsBetween(-1, Window.currentRow)

filtered_readings = (
    df_meter_readings
    .filter(F.col("voltage") <= 231.6)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

with_trends = (
    filtered_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        F.size(F.collect_set("quality").over(w_site_id)).alias("distinct_quality_in_site_id"),
        F.nth_value("kwh", 2).over(w_site_id_by_read_at_full).alias("second_kwh"),
        F.round(F.avg("voltage").over(w_all_by_voltage_prior_1), 2).alias("voltage_avg_1_prior"),
        F.min("voltage").over(w_site_id_read_at_to_date).alias("site_id_read_at_to_date_min_voltage"),
        F.round(F.cume_dist().over(w_site_id_by_read_at_ties), 3).alias("read_at_cume_dist"),
        F.rank().over(w_site_id_by_read_at).alias("read_at_rank"),
        F.round(F.col("voltage") / F.nullif(F.sum("voltage").over(w_quality_site_id), F.lit(0)), 4).alias("voltage_share_of_quality_site_id"),
        F.row_number().over(w_all_by_voltage).alias("voltage_seq"),
        F.round(F.percent_rank().over(w_read_at_to_date_quality_by_k), 3).alias("kwh_percent_rank"),
        F.max("voltage").over(w_quality_site_id).alias("quality_site_id_max_voltage"),
        F.round(F.cume_dist().over(w_all_by_voltage_ties), 3).alias("voltage_cume_dist"),
        F.min("voltage").over(w_all_by_voltage_running).alias("min_voltage_so_far"),
        F.round(F.col("voltage") / F.nullif(F.sum("voltage").over(w_site_id_read_at_to_date), F.lit(0)), 4).alias("voltage_share_of_site_id_read_at_to_date"),
        F.size(F.collect_set("quality").over(w_site_id_read_at_to_date)).alias("distinct_quality_in_site_id_read_at_to_date"),
        F.round(F.avg("voltage").over(w_quality_site_id), 2).alias("quality_site_id_avg_voltage"),
        F.count(F.lit(1)).over(w_site_id).alias("rows_in_site_id"),
    )
)

sequenced = (
    with_trends
    .select(
        "*",
        F.row_number().over(w_site_id_read_at_rank_by_seco).alias("seq_no"),
        F.lead("read_at_rank", 1, 0).over(w_distinct_quality_in_site_id).alias("next_read_at_rank"),
        F.ntile(5).over(w_distinct_quality_in_site_id).alias("voltage_share_of_quality_site_id_quintile"),
        F.max("kwh_percent_rank").over(w_distinct_quality_in_site_id_2).alias("kwh_percent_rank_max_2"),
    )
)

result = (
    sequenced
    .filter(F.col("read_at_rank") >= 5)
    .select(
        "id",
        "site_id",
        "distinct_quality_in_site_id",
        "second_kwh",
        "voltage_avg_1_prior",
        "site_id_read_at_to_date_min_voltage",
        "read_at_rank",
        "voltage_share_of_quality_site_id",
        "voltage_seq",
        "voltage_cume_dist",
        "min_voltage_so_far",
        "distinct_quality_in_site_id_read_at_to_date",
        "rows_in_site_id",
        "seq_no",
        "voltage_share_of_quality_site_id_quintile",
        "kwh_percent_rank_max_2",
        F.round(F.col("kwh") * 0.9, 2).alias("kwh_adj"),
        F.month(F.col("read_at")).alias("read_at_month"),
    )
    .orderBy(
        F.col("min_voltage_so_far").asc(),
        F.col("site_id_read_at_to_date_min_voltage").desc_nulls_first(),
        "id",
    )
    .limit(10)
)
