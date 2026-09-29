from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_by_voltage_running = Window.partitionBy("site_id").orderBy("voltage", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_id_quality_by_kwh_runni = Window.partitionBy("site_id", "quality").orderBy(F.col("kwh").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_id_quality_by_kwh = Window.partitionBy("site_id", "quality").orderBy(F.col("kwh").desc_nulls_first(), "id")
w_site_id_read_at_month_by_rea = Window.partitionBy("site_id", F.month("read_at")).orderBy("read_at", "id")
w_site_id_read_at_month_by_rea_2 = Window.partitionBy("site_id", F.month("read_at")).orderBy("read_at")
w_quality_by_read_at = Window.partitionBy("quality").orderBy(F.col("read_at").asc_nulls_last(), "id")
w_site_id = Window.partitionBy("site_id")
w_all_by_read_at = Window.orderBy("read_at", "id")

scoped_readings = (
    df_meter_readings
    .filter((F.col("site_id").isNotNull()) & (F.col("voltage") >= 220.2))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
        F.initcap(F.trim(F.col("quality"))).alias("quality_clean"),
    )
)

ranked = (
    scoped_readings
    .select(
        "id",
        "read_at",
        "kwh",
        "voltage",
        "quality_slug",
        "quality_clean",
        F.last("voltage", ignorenulls=True).over(w_site_id_by_voltage_running).alias("last_known_voltage"),
        F.first("voltage", ignorenulls=True).over(w_site_id_quality_by_kwh_runni).alias("first_known_voltage"),
        F.dense_rank().over(w_site_id_quality_by_kwh).alias("kwh_dense_rank"),
        F.ntile(3).over(w_site_id_read_at_month_by_rea).alias("read_at_tercile"),
        F.last("kwh", ignorenulls=True).over(w_site_id_quality_by_kwh_runni).alias("last_known_kwh"),
        F.max("voltage").over(w_site_id_by_voltage_running).alias("max_voltage_so_far"),
        F.round(F.percent_rank().over(w_site_id_read_at_month_by_rea_2), 3).alias("read_at_percent_rank"),
        F.rank().over(w_quality_by_read_at).alias("read_at_rank"),
        F.round(F.col("kwh") - F.avg("kwh").over(w_site_id), 2).alias("kwh_vs_site_id_avg"),
        F.sum("kwh").over(w_site_id).alias("site_id_sum_kwh"),
        F.lag("voltage").over(w_all_by_read_at).alias("prev_voltage"),
        F.round(F.percent_rank().over(w_all_by_read_at), 3).alias("read_at_percent_rank_2"),
    )
)

top_per_group = ranked.filter((F.col("kwh_dense_rank") <= 5) & (F.col("read_at_percent_rank") < 0.9))

result = (
    top_per_group
    .select(
        "id",
        "kwh",
        "voltage",
        "quality_slug",
        "quality_clean",
        "first_known_voltage",
        "kwh_dense_rank",
        "read_at_tercile",
        "last_known_kwh",
        "max_voltage_so_far",
        "prev_voltage",
        "read_at_percent_rank_2",
        F.to_date(F.col("read_at")).alias("read_at_date"),
    )
)
