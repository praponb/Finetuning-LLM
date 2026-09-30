from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_year_by_read_at = Window.partitionBy(F.year("read_at")).orderBy(F.col("read_at").desc(), "id")
w_site_id = Window.partitionBy("site_id")
w_read_at_year_by_read_at_ties = Window.partitionBy(F.year("read_at")).orderBy(F.col("read_at").desc())
w_site_id_by_kwh_ties = Window.partitionBy("site_id").orderBy("kwh")
w_read_at_to_date_site_id = Window.partitionBy(F.to_date("read_at"), "site_id")
w_site_id_by_read_at = Window.partitionBy("site_id").orderBy(F.col("read_at").desc(), "id")
w_site_id_by_kwh = Window.partitionBy("site_id").orderBy("kwh", "id")

recent_readings = (
    df_meter_readings
    .filter(F.col("site_id") >= 2)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.substring(F.trim(F.col("quality")), 1, 2).alias("quality_prefix"),
    )
)

windowed = (
    recent_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "quality_prefix",
        F.ntile(10).over(w_read_at_year_by_read_at).alias("read_at_decile"),
        F.count(F.lit(1)).over(w_site_id).alias("rows_in_site_id"),
        F.round(F.percent_rank().over(w_read_at_year_by_read_at_ties), 3).alias("read_at_percent_rank"),
        F.min("voltage").over(w_site_id).alias("site_id_min_voltage"),
        F.rank().over(w_site_id_by_kwh_ties).alias("kwh_rank"),
        F.round(F.col("kwh") - F.avg("kwh").over(w_read_at_to_date_site_id), 2).alias("kwh_vs_read_at_to_date_site_id_avg"),
        F.lead("kwh", 2).over(w_site_id_by_read_at).alias("next2_kwh"),
        F.lag("voltage", 1, 0).over(w_site_id_by_kwh).alias("prev_voltage"),
    )
)

leaders = windowed.filter(F.col("kwh_rank") <= 2)

result = (
    leaders
    .select(
        "id",
        "site_id",
        "read_at",
        "quality_prefix",
        "read_at_decile",
        "rows_in_site_id",
        "read_at_percent_rank",
        "kwh_rank",
        "kwh_vs_read_at_to_date_site_id_avg",
        "next2_kwh",
        "prev_voltage",
    )
    .orderBy("next2_kwh", "site_id")
)
