from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_year_site_id = Window.partitionBy(F.year("read_at"), "site_id")
w_quality_by_voltage_ties = Window.partitionBy("quality").orderBy(F.col("voltage").asc_nulls_last())
w_all_by_voltage_ties = Window.orderBy(F.col("voltage").asc_nulls_last())
w_site_id_quality_by_read_at_t = Window.partitionBy("site_id", "quality").orderBy("read_at")
w_all_by_voltage = Window.orderBy("voltage", "id")
w_site_id_quality_by_read_at_f = Window.partitionBy("site_id", "quality").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_quality_by_voltage = Window.partitionBy("quality").orderBy(F.col("voltage").asc_nulls_last(), "id")
w_quality_site_id = Window.partitionBy("quality", "site_id")
w_all_by_voltage_2 = Window.orderBy(F.col("voltage").asc_nulls_last(), "id")
w_site_id_quality_by_read_at = Window.partitionBy("site_id", "quality").orderBy("read_at", "id")
w_quality_read_at_month_by_rea = Window.partitionBy("quality", F.month("read_at")).orderBy(F.col("read_at_cume_dist").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_voltage_rank_ties = Window.orderBy("voltage_rank")
w_site_id_by_voltage_ties = Window.partitionBy("site_id").orderBy("voltage")
w_all_by_voltage_rank_running = Window.orderBy("voltage_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped_readings = (
    df_meter_readings
    .filter((F.col("site_id").isNotNull()) | (F.col("read_at") < F.lit("2025-02-18 12:00:00").cast("timestamp")))
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
    )
)

sequenced = (
    scoped_readings
    .select(
        "*",
        F.round(F.col("voltage") - F.avg("voltage").over(w_read_at_year_site_id), 2).alias("voltage_vs_read_at_year_site_id_avg"),
        F.round(F.cume_dist().over(w_quality_by_voltage_ties), 3).alias("voltage_cume_dist"),
        F.rank().over(w_all_by_voltage_ties).alias("voltage_rank"),
        F.round(F.cume_dist().over(w_site_id_quality_by_read_at_t), 3).alias("read_at_cume_dist"),
        F.round((F.unix_timestamp("read_at") - F.unix_timestamp(F.lag("read_at").over(w_all_by_voltage))) / 60, 1).alias("minutes_since_prev_read_at"),
        F.last("read_at").over(w_site_id_quality_by_read_at_f).alias("last_read_at"),
        F.lead("read_at", 2).over(w_quality_by_voltage).alias("next2_read_at"),
        F.round(F.col("voltage") / F.nullif(F.sum("voltage").over(w_quality_site_id), F.lit(0)), 4).alias("voltage_share_of_quality_site_id"),
        F.lead("read_at", 3).over(w_all_by_voltage_2).alias("next3_read_at"),
        F.lead("voltage").over(w_site_id_quality_by_read_at).alias("next_voltage"),
    )
)

with_trends = (
    sequenced
    .select(
        "*",
        F.last("next3_read_at", ignorenulls=True).over(w_quality_read_at_month_by_rea).alias("last_known_next3_read_at"),
        F.last("next_voltage", ignorenulls=True).over(w_quality_read_at_month_by_rea).alias("last_known_next_voltage"),
        F.sum("read_at_cume_dist").over(w_quality_read_at_month_by_rea).alias("running_read_at_cume_dist"),
        F.rank().over(w_all_by_voltage_rank_ties).alias("voltage_rank_rank"),
        F.round(F.cume_dist().over(w_site_id_by_voltage_ties), 3).alias("voltage_cume_dist_2"),
        F.first("minutes_since_prev_read_at", ignorenulls=True).over(w_all_by_voltage_rank_running).alias("first_known_minutes_since_prev_read_at"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "read_at",
        "voltage",
        "quality",
        "quality_slug",
        "voltage_vs_read_at_year_site_id_avg",
        "voltage_cume_dist",
        "voltage_rank",
        "last_read_at",
        "voltage_share_of_quality_site_id",
        "next3_read_at",
        "next_voltage",
        "last_known_next3_read_at",
        "last_known_next_voltage",
        "voltage_rank_rank",
        "voltage_cume_dist_2",
        "first_known_minutes_since_prev_read_at",
        F.coalesce(F.col("voltage_cume_dist_2"), F.lit(0.0)).alias("voltage_cume_dist_2_filled"),
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug_2"),
    )
    .orderBy(F.col("voltage_cume_dist_2").asc())
)
