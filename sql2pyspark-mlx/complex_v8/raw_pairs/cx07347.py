from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_by_read_at_ties = Window.partitionBy("site_id").orderBy(F.col("read_at").desc_nulls_first())
w_site_id = Window.partitionBy("site_id")
w_all_by_kwh_distance_prior_1 = Window.orderBy(F.col("kwh_distance").desc(), "id").rowsBetween(-1, -1)
w_read_at_month_by_voltage_run = Window.partitionBy(F.month("read_at")).orderBy("voltage", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_read_at_month_by_voltage_mov = Window.partitionBy(F.month("read_at")).orderBy("voltage", "id").rowsBetween(-3, Window.currentRow)
w_all_by_kwh_distance = Window.orderBy(F.col("kwh_distance").desc(), "id")
w_site_id_by_kwh_distance = Window.partitionBy("site_id").orderBy(F.col("kwh_distance").desc(), "id")
w_read_at_month_by_voltage = Window.partitionBy(F.month("read_at")).orderBy("voltage", "id")
w_kwh_distance_dense_rank_by_n = Window.partitionBy("kwh_distance_dense_rank").orderBy(F.col("next3_kwh_distance").desc(), "id").rowsBetween(-6, -1)
w_read_at_year = Window.partitionBy(F.year("read_at"))
w_all_by_site_id_max_kwh_dista = Window.orderBy(F.col("site_id_max_kwh_distance").desc(), "id")

filtered = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.abs(F.col("kwh") - 52.5).alias("kwh_distance"),
    )
)

windowed = (
    filtered
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "kwh_distance",
        F.round(F.cume_dist().over(w_site_id_by_read_at_ties), 3).alias("read_at_cume_dist"),
        F.count(F.lit(1)).over(w_site_id).alias("rows_in_site_id"),
        F.sum("kwh").over(w_all_by_kwh_distance_prior_1).alias("kwh_sum_1_prior"),
        F.round(F.col("kwh_distance") - F.avg("kwh_distance").over(w_site_id), 2).alias("kwh_distance_vs_site_id_avg"),
        F.last("voltage", ignorenulls=True).over(w_read_at_month_by_voltage_run).alias("last_known_voltage"),
        F.round(F.avg("kwh").over(w_read_at_month_by_voltage_mov), 2).alias("kwh_avg_4"),
        F.lead("kwh_distance", 3, 0).over(w_all_by_kwh_distance).alias("next3_kwh_distance"),
        F.min("voltage").over(w_site_id).alias("site_id_min_voltage"),
        F.max("kwh_distance").over(w_site_id).alias("site_id_max_kwh_distance"),
        F.round(F.avg("voltage").over(w_site_id), 2).alias("site_id_avg_voltage"),
        F.round(F.col("kwh_distance") / F.nullif(F.sum("kwh_distance").over(w_site_id), F.lit(0)), 4).alias("kwh_distance_share_of_site_id"),
        F.dense_rank().over(w_site_id_by_kwh_distance).alias("kwh_distance_dense_rank"),
        F.row_number().over(w_read_at_month_by_voltage).alias("voltage_seq"),
    )
)

scored = (
    windowed
    .withColumn("site_id_max_kwh_distance_adj", F.round(F.col("site_id_max_kwh_distance") * 1.5, 2))
)

with_trends = (
    scored
    .select(
        "id",
        "read_at",
        "kwh",
        "voltage",
        "kwh_distance",
        "read_at_cume_dist",
        "kwh_distance_vs_site_id_avg",
        "last_known_voltage",
        "next3_kwh_distance",
        "site_id_min_voltage",
        "site_id_max_kwh_distance",
        "kwh_distance_share_of_site_id",
        "kwh_distance_dense_rank",
        "voltage_seq",
        "site_id_max_kwh_distance_adj",
        F.max("kwh_distance_dense_rank").over(w_kwh_distance_dense_rank_by_n).alias("kwh_distance_dense_rank_max_6_prior"),
        F.round(F.col("voltage_seq") / F.nullif(F.sum("voltage_seq").over(w_read_at_year), F.lit(0)), 4).alias("voltage_seq_share_of_read_at_year"),
        F.size(F.collect_set("site_id").over(w_read_at_year)).alias("distinct_site_id_in_read_at_year"),
        F.lead("kwh_distance", 3).over(w_all_by_site_id_max_kwh_dista).alias("next3_kwh_distance_2"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "kwh",
        "kwh_distance",
        "read_at_cume_dist",
        "kwh_distance_vs_site_id_avg",
        "last_known_voltage",
        "next3_kwh_distance",
        "site_id_max_kwh_distance",
        "kwh_distance_dense_rank",
        "voltage_seq_share_of_read_at_year",
        "distinct_site_id_in_read_at_year",
        "next3_kwh_distance_2",
        F.least(F.col("read_at_cume_dist"), F.col("kwh_distance_dense_rank")).alias("least_read_at_cume_dist_kwh_distance_dense_rank"),
    )
    .orderBy(F.col("last_known_voltage").desc_nulls_first())
)
