from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_read_at = Window.orderBy("read_at", "id")
w_site_id_by_voltage = Window.partitionBy("site_id").orderBy(F.col("voltage").asc_nulls_last(), "id")
w_all_by_voltage_filled_full = Window.orderBy(F.col("voltage_filled").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_id = Window.partitionBy("site_id")
w_all_by_voltage_filled = Window.orderBy(F.col("voltage_filled").desc(), "id")
w_site_id_by_voltage_moving_3 = Window.partitionBy("site_id").orderBy(F.col("voltage").desc(), "id").rowsBetween(-3, Window.currentRow)

valid_readings = (
    df_meter_readings
    .filter(F.col("kwh").between(35.9, 109.0))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.coalesce(F.col("kwh"), F.lit(0.0)).alias("kwh_filled"),
        F.coalesce(F.col("voltage"), F.lit(0.0)).alias("voltage_filled"),
    )
)

with_trends = (
    valid_readings
    .select(
        "*",
        F.lead("voltage").over(w_all_by_read_at).alias("next_voltage"),
        F.lag("kwh", 3, 0).over(w_site_id_by_voltage).alias("prev3_kwh"),
        F.nth_value("read_at", 2).over(w_all_by_voltage_filled_full).alias("second_read_at"),
        F.round(F.col("kwh_filled") / F.nullif(F.sum("kwh_filled").over(w_site_id), F.lit(0)), 4).alias("kwh_filled_share_of_site_id"),
        F.round(F.col("voltage_filled") / F.nullif(F.sum("voltage_filled").over(w_site_id), F.lit(0)), 4).alias("voltage_filled_share_of_site_id"),
        F.nth_value("voltage_filled", 2).over(w_all_by_voltage_filled_full).alias("second_voltage_filled"),
        F.dense_rank().over(w_all_by_voltage_filled).alias("voltage_filled_dense_rank"),
        F.max("kwh_filled").over(w_site_id_by_voltage_moving_3).alias("kwh_filled_max_4"),
    )
)

top_per_group = (
    with_trends
    .filter(F.col("voltage_filled_dense_rank") <= 3)
    .drop("voltage_filled_dense_rank")
)

result = (
    top_per_group
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "voltage_filled",
        "next_voltage",
        "prev3_kwh",
        "second_read_at",
        "kwh_filled_share_of_site_id",
        "voltage_filled_share_of_site_id",
        "second_voltage_filled",
        "kwh_filled_max_4",
        F.greatest(F.col("second_voltage_filled"), F.col("voltage_filled")).alias("greatest_second_voltage_filled_voltage_filled"),
    )
    .orderBy("greatest_second_voltage_filled_voltage_filled", "id")
    .limit(5)
)
