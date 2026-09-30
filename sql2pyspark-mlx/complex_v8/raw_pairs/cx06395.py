from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_read_at = Window.orderBy(F.col("read_at").desc(), "id")
w_site_id_by_read_at = Window.partitionBy("site_id").orderBy("read_at", "id")
w_site_zone_site_id_by_read_at = Window.partitionBy("site_zone", "site_id").orderBy(F.col("read_at").desc(), "id")
w_all_by_read_at_running = Window.orderBy(F.col("read_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_id_by_read_at_ties = Window.partitionBy("site_id").orderBy("read_at")

recent = (
    df_meter_readings
    .filter((F.col("kwh") >= 20.4) & (F.col("site_id").isNotNull()))
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
    )
)

with_details_site = (
    recent.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.quality",
        "si.site_name",
        F.col("si.zone").alias("site_zone"),
    )
)

with_windows = (
    with_details_site
    .select(
        "*",
        F.row_number().over(w_all_by_read_at).alias("read_at_seq"),
        F.lead("read_at").over(w_site_id_by_read_at).alias("next_read_at"),
        F.round((F.unix_timestamp("read_at") - F.unix_timestamp(F.lag("read_at").over(w_site_zone_site_id_by_read_at))) / 60, 1).alias("minutes_since_prev_read_at"),
        F.first("read_at", ignorenulls=True).over(w_all_by_read_at_running).alias("first_known_read_at"),
        F.lead("read_at").over(w_all_by_read_at).alias("next_read_at_2"),
        F.round(F.cume_dist().over(w_site_id_by_read_at_ties), 3).alias("read_at_cume_dist"),
        F.ntile(4).over(w_all_by_read_at).alias("read_at_quartile"),
        F.round(F.cume_dist().over(w_all_by_read_at), 3).alias("read_at_cume_dist_2"),
    )
)

first_rows = (
    with_windows
    .filter((F.col("read_at_seq") <= 2) & (F.col("minutes_since_prev_read_at") > 2000.0))
)

result = (
    first_rows
    .select(
        "id",
        "read_at",
        "quality",
        "site_name",
        "minutes_since_prev_read_at",
        "read_at_quartile",
        "read_at_cume_dist_2",
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
    )
)
