from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_by_kwh_moving_6 = Window.partitionBy("quality").orderBy("kwh", "id").rowsBetween(-6, Window.currentRow)
w_quality_by_kwh = Window.partitionBy("quality").orderBy("kwh", "id")
w_site_id = Window.partitionBy("site_id")
w_quality_group_quality = Window.partitionBy("quality_group", "quality")
w_all_by_kwh_ties = Window.orderBy("kwh")
w_quality_quality_group = Window.partitionBy("quality", "quality_group")
w_quality_group_by_read_at_yea = Window.partitionBy("quality_group").orderBy(F.col("read_at_year").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_quality_group_by_read_at_yea_2 = Window.partitionBy("quality_group").orderBy(F.col("read_at_year").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_distinct_site_id_in_q = Window.orderBy(F.col("distinct_site_id_in_quality_quality_group").desc(), "id")
w_distinct_quality_group_in_si = Window.partitionBy("distinct_quality_group_in_site_id").orderBy("voltage", "id").rowsBetween(-2, -1)
w_quality_group_kwh_rank_by_di = Window.partitionBy("quality_group", "kwh_rank").orderBy("distinct_quality_group_in_site_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_distinct_quality_group_in_si_2 = Window.partitionBy("distinct_quality_group_in_site_id").orderBy("voltage", "id").rowsBetween(-6, -1)

prepared = (
    df_meter_readings
    .filter((F.col("voltage") >= 222.6) & (F.col("kwh") > 20.4))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.when(F.col("quality").isin("OK", "BAD"), "group_a").when(F.col("quality") == "EST", "group_b").otherwise("other").alias("quality_group"),
        F.date_trunc("year", F.col("read_at")).alias("read_at_year"),
    )
)

ranked = (
    prepared
    .select(
        "*",
        F.round(F.avg("voltage").over(w_quality_by_kwh_moving_6), 2).alias("voltage_avg_7"),
        F.row_number().over(w_quality_by_kwh).alias("rn"),
        F.size(F.collect_set("quality_group").over(w_site_id)).alias("distinct_quality_group_in_site_id"),
        F.count(F.lit(1)).over(w_quality_group_quality).alias("rows_in_quality_group_quality"),
        F.max("voltage").over(w_quality_group_quality).alias("quality_group_quality_max_voltage"),
        F.rank().over(w_all_by_kwh_ties).alias("kwh_rank"),
        F.size(F.collect_set("site_id").over(w_quality_quality_group)).alias("distinct_site_id_in_quality_quality_group"),
        F.last("read_at_year").over(w_quality_group_by_read_at_yea).alias("last_read_at_year"),
        F.count(F.lit(1)).over(w_quality_group_by_read_at_yea_2).alias("running_count"),
    )
)

leaders = ranked.filter(F.col("kwh_rank") == 1)

windowed = (
    leaders
    .select(
        "*",
        F.ntile(10).over(w_all_by_distinct_site_id_in_q).alias("distinct_site_id_in_quality_quality_group_decile"),
        F.sum("voltage_avg_7").over(w_distinct_quality_group_in_si).alias("voltage_avg_7_sum_2_prior"),
        F.last("kwh_rank", ignorenulls=True).over(w_quality_group_kwh_rank_by_di).alias("last_known_kwh_rank"),
        F.max("rn").over(w_quality_group_kwh_rank_by_di).alias("max_rn_so_far"),
        F.lag("last_read_at_year").over(w_all_by_distinct_site_id_in_q).alias("prev_last_read_at_year"),
        F.max("kwh").over(w_distinct_quality_group_in_si_2).alias("kwh_max_6_prior"),
    )
)

result = (
    windowed
    .select(
        "id",
        "site_id",
        "read_at",
        "quality",
        "quality_group",
        "voltage_avg_7",
        "rn",
        "distinct_quality_group_in_site_id",
        "rows_in_quality_group_quality",
        "kwh_rank",
        "distinct_site_id_in_quality_quality_group",
        "last_read_at_year",
        "running_count",
        "last_known_kwh_rank",
        "prev_last_read_at_year",
        "kwh_max_6_prior",
        F.round(F.col("rn") / F.nullif(F.col("voltage"), F.lit(0)), 3).alias("rn_per_voltage"),
    )
    .orderBy(F.col("quality_group").desc(), F.col("site_id").desc())
)
