from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_to_date_quality = Window.partitionBy(F.to_date("read_at"), "quality")
w_all_by_kwh_full = Window.orderBy(F.col("kwh").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_read_at_ties = Window.orderBy(F.col("read_at").desc_nulls_first())
w_quality_by_read_at = Window.partitionBy("quality").orderBy(F.col("read_at").desc(), "id")
w_quality_site_id_by_kwh_movin = Window.partitionBy("quality", "site_id").orderBy(F.col("kwh").desc(), "id").rowsBetween(-2, Window.currentRow)
w_all_by_kwh = Window.orderBy(F.col("kwh").desc(), "id")
w_all_by_read_at_moving_1 = Window.orderBy(F.col("read_at").desc_nulls_first(), "id").rowsBetween(-1, Window.currentRow)
w_site_id_quality = Window.partitionBy("site_id", "quality")

recent = (
    df_meter_readings
    .filter(F.hour(F.col("read_at")).between(8, 11))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
    )
)

windowed = (
    recent
    .select(
        "id",
        "site_id",
        "read_at",
        F.count(F.lit(1)).over(w_read_at_to_date_quality).alias("rows_in_read_at_to_date_quality"),
        F.last("kwh").over(w_all_by_kwh_full).alias("last_kwh"),
        F.round(F.percent_rank().over(w_all_by_read_at_ties), 3).alias("read_at_percent_rank"),
        F.round(F.cume_dist().over(w_quality_by_read_at), 3).alias("read_at_cume_dist"),
        F.max("kwh").over(w_quality_site_id_by_kwh_movin).alias("kwh_max_3"),
        F.ntile(3).over(w_all_by_kwh).alias("kwh_tercile"),
        F.sum("kwh").over(w_read_at_to_date_quality).alias("read_at_to_date_quality_sum_kwh"),
        F.round(F.avg("kwh").over(w_all_by_read_at_moving_1), 2).alias("kwh_avg_2"),
        F.count(F.lit(1)).over(w_site_id_quality).alias("rows_in_site_id_quality"),
    )
)

scored = windowed.withColumn("kwh_tercile_distance", F.abs(F.col("kwh_tercile") - 4))

result = (
    scored
    .select(
        "id",
        "read_at",
        "last_kwh",
        "read_at_percent_rank",
        "read_at_cume_dist",
        "kwh_tercile",
        "read_at_to_date_quality_sum_kwh",
        "kwh_avg_2",
        F.round(F.col("kwh_avg_2") / F.nullif(F.col("read_at_cume_dist"), F.lit(0)), 3).alias("kwh_avg_2_per_read_at_cume_dist"),
        F.quarter(F.col("read_at")).alias("read_at_quarter"),
    )
    .orderBy(F.col("read_at_to_date_quality_sum_kwh").desc(), "last_kwh")
)
