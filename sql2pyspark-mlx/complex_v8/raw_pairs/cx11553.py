from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_by_kwh_moving_3 = Window.partitionBy("site_id").orderBy("kwh", "id").rowsBetween(-3, Window.currentRow)
w_all_by_kwh = Window.orderBy(F.col("kwh").desc(), "id")
w_site_id_by_kwh_full = Window.partitionBy("site_id").orderBy("kwh", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_kwh_running = Window.orderBy(F.col("kwh").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_id_quality = Window.partitionBy("site_id", "quality")
w_all_by_kwh_ties = Window.orderBy(F.col("kwh").desc())
w_quality_by_read_at = Window.partitionBy("quality").orderBy("read_at", "id")
w_quality = Window.partitionBy("quality")
w_site_id_quality_by_read_at_m = Window.partitionBy("site_id", "quality").orderBy(F.col("read_at").desc(), "id").rowsBetween(-2, Window.currentRow)
w_site_id_by_kwh = Window.partitionBy("site_id").orderBy("kwh", "id")

filtered = (
    df_meter_readings
    .filter((F.col("voltage") > 214.3) & (F.col("site_id").isNotNull()))
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
    filtered
    .select(
        "id",
        "site_id",
        "read_at",
        "quality",
        "quality_slug",
        F.max("voltage").over(w_site_id_by_kwh_moving_3).alias("voltage_max_4"),
        F.row_number().over(w_all_by_kwh).alias("seq_no"),
        F.nth_value("read_at", 2).over(w_site_id_by_kwh_full).alias("second_read_at"),
        F.last("read_at", ignorenulls=True).over(w_all_by_kwh_running).alias("last_known_read_at"),
        F.round(F.col("kwh") / F.nullif(F.sum("kwh").over(w_site_id_quality), F.lit(0)), 4).alias("kwh_share_of_site_id_quality"),
        F.rank().over(w_all_by_kwh_ties).alias("kwh_rank"),
        F.round(F.avg("kwh").over(w_site_id_quality), 2).alias("site_id_quality_avg_kwh"),
        F.round(F.cume_dist().over(w_quality_by_read_at), 3).alias("read_at_cume_dist"),
        F.round(F.col("kwh") / F.nullif(F.sum("kwh").over(w_quality), F.lit(0)), 4).alias("kwh_share_of_quality"),
        F.sum("voltage").over(w_quality).alias("quality_sum_voltage"),
        F.max("voltage").over(w_site_id_quality_by_read_at_m).alias("voltage_max_3"),
        F.lag("read_at", 3).over(w_site_id_by_kwh).alias("prev3_read_at"),
    )
)

top_rows = sequenced.filter(F.col("seq_no") == 1)

result = (
    top_rows
    .select(
        "id",
        "site_id",
        "quality",
        "quality_slug",
        "voltage_max_4",
        "seq_no",
        "second_read_at",
        "last_known_read_at",
        "kwh_share_of_site_id_quality",
        "kwh_rank",
        "site_id_quality_avg_kwh",
        "kwh_share_of_quality",
        "voltage_max_3",
    )
    .orderBy("site_id", F.col("voltage_max_4").desc())
)
