from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality = Window.partitionBy("quality")
w_all_by_voltage_ties = Window.orderBy(F.col("voltage").desc())
w_all_by_voltage = Window.orderBy("voltage", "id")
w_quality_by_voltage = Window.partitionBy("quality").orderBy(F.col("voltage").desc(), "id")
w_all_by_voltage_moving_6 = Window.orderBy(F.col("voltage").desc(), "id").rowsBetween(-6, Window.currentRow)
w_quality_by_voltage_full = Window.partitionBy("quality").orderBy(F.col("voltage").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_voltage_running = Window.orderBy(F.col("voltage").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_min_voltage_so_far = Window.orderBy(F.col("min_voltage_so_far").desc(), "id")
w_voltage_dense_rank_quality = Window.partitionBy("voltage_dense_rank", "quality")
w_all_by_voltage_cume_dist_adj = Window.orderBy(F.col("voltage_cume_dist_adj").desc(), "id")

base = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
    )
)

sequenced = (
    base
    .select(
        "*",
        F.sum("voltage").over(w_quality).alias("quality_sum_voltage"),
        F.dense_rank().over(w_all_by_voltage_ties).alias("voltage_dense_rank"),
        F.lag("read_at").over(w_all_by_voltage).alias("prev_read_at"),
        F.round(F.cume_dist().over(w_all_by_voltage), 3).alias("voltage_cume_dist"),
        F.size(F.collect_set("site_id").over(w_quality)).alias("distinct_site_id_in_quality"),
        F.row_number().over(w_quality_by_voltage).alias("seq_no"),
        F.count(F.lit(1)).over(w_quality).alias("rows_in_quality"),
        F.round(F.col("voltage") - F.avg("voltage").over(w_quality), 2).alias("voltage_vs_quality_avg"),
        F.round(F.avg("voltage").over(w_all_by_voltage_moving_6), 2).alias("voltage_avg_7"),
        F.last("kwh").over(w_quality_by_voltage_full).alias("last_kwh"),
        F.min("voltage").over(w_all_by_voltage_running).alias("min_voltage_so_far"),
        (F.col("kwh") - F.lag("kwh").over(w_quality_by_voltage)).alias("kwh_change"),
        F.dense_rank().over(w_all_by_voltage).alias("voltage_dense_rank_2"),
    )
)

tagged = (
    sequenced
    .filter(F.col("kwh") < 87.1)
    .select(
        "*",
        F.round(F.col("voltage_cume_dist") * 1.1, 2).alias("voltage_cume_dist_adj"),
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
    )
)

sequenced_2 = (
    tagged
    .select(
        "id",
        "read_at",
        "kwh",
        "quality_slug",
        "quality_sum_voltage",
        "voltage_dense_rank",
        "prev_read_at",
        "voltage_cume_dist",
        "distinct_site_id_in_quality",
        "rows_in_quality",
        "voltage_avg_7",
        "last_kwh",
        "min_voltage_so_far",
        "kwh_change",
        "voltage_dense_rank_2",
        "voltage_cume_dist_adj",
        "quality_or_unknown",
        F.row_number().over(w_all_by_min_voltage_so_far).alias("seq_no_2"),
        F.max("voltage_avg_7").over(w_voltage_dense_rank_quality).alias("voltage_dense_rank_quality_max_voltage_avg_7"),
        F.lead("read_at").over(w_all_by_voltage_cume_dist_adj).alias("next_read_at"),
    )
)

result = (
    sequenced_2
    .select(
        "id",
        "read_at",
        "kwh",
        "quality_sum_voltage",
        "voltage_dense_rank",
        "prev_read_at",
        "distinct_site_id_in_quality",
        "rows_in_quality",
        "voltage_avg_7",
        "last_kwh",
        "min_voltage_so_far",
        "voltage_cume_dist_adj",
        "quality_or_unknown",
        "voltage_dense_rank_quality_max_voltage_avg_7",
        "next_read_at",
        F.round((F.unix_timestamp(F.col("prev_read_at")) - F.unix_timestamp(F.col("next_read_at"))) / 60, 1).alias("minutes_next_read_at_to_prev_read_at"),
        (F.col("voltage_dense_rank") + F.col("min_voltage_so_far")).alias("voltage_dense_rank_plus_min_voltage_so_far"),
    )
)
