from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id")
w_all_by_kwh_full = Window.orderBy(F.col("kwh").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_read_at_plus_7d_to_date_by_v = Window.partitionBy(F.to_date("read_at_plus_7d")).orderBy(F.col("voltage").desc(), "id")
w_read_at_plus_7d_to_date_by_v_2 = Window.partitionBy(F.to_date("read_at_plus_7d")).orderBy(F.col("voltage").desc())

prepared_readings = (
    df_meter_readings
    .filter((F.col("quality") == "BAD") & (F.col("site_id") <= 9))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        (F.col("read_at") + F.expr("INTERVAL 7 DAYS")).alias("read_at_plus_7d"),
    )
)

sequenced = (
    prepared_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "read_at_plus_7d",
        F.round(F.col("voltage") / F.nullif(F.sum("voltage").over(w_site_id), F.lit(0)), 4).alias("voltage_share_of_site_id"),
        F.nth_value("read_at_plus_7d", 2).over(w_all_by_kwh_full).alias("second_read_at_plus_7d"),
        F.round((F.unix_timestamp("read_at") - F.unix_timestamp(F.lag("read_at").over(w_read_at_plus_7d_to_date_by_v))) / 60, 1).alias("minutes_since_prev_read_at"),
        F.round(F.cume_dist().over(w_read_at_plus_7d_to_date_by_v_2), 3).alias("voltage_cume_dist"),
    )
)

tagged = (
    sequenced
    .filter(F.col("voltage_cume_dist").between(0.2, 0.9))
    .select(
        "id",
        "read_at",
        "read_at_plus_7d",
        "voltage_share_of_site_id",
        "second_read_at_plus_7d",
        "minutes_since_prev_read_at",
        "voltage_cume_dist",
        F.date_trunc("year", F.col("read_at")).alias("read_at_year"),
        (F.col("kwh") + F.col("voltage_cume_dist")).alias("kwh_plus_voltage_cume_dist"),
        F.round(F.col("voltage_cume_dist") / F.nullif(F.col("voltage"), F.lit(0)), 3).alias("voltage_cume_dist_per_voltage"),
    )
)

with_activity = (
    tagged
    .crossJoin(tagged.agg(F.avg("minutes_since_prev_read_at").alias("overall_avg_minutes_since_prev_read_at")))
    .filter(F.col("minutes_since_prev_read_at") > F.col("overall_avg_minutes_since_prev_read_at"))
    .drop("overall_avg_minutes_since_prev_read_at")
)

screened = (
    with_activity
    .crossJoin(with_activity.agg(F.max("minutes_since_prev_read_at").alias("overall_max_minutes_since_prev_read_at")))
    .filter(F.col("minutes_since_prev_read_at") < F.col("overall_max_minutes_since_prev_read_at"))
    .drop("overall_max_minutes_since_prev_read_at")
)

result = (
    screened
    .filter(F.col("minutes_since_prev_read_at") < 3000.0)
    .select(
        "id",
        "read_at",
        "read_at_plus_7d",
        "voltage_share_of_site_id",
        "second_read_at_plus_7d",
        "minutes_since_prev_read_at",
        "voltage_cume_dist",
        "read_at_year",
        "kwh_plus_voltage_cume_dist",
        "voltage_cume_dist_per_voltage",
        F.greatest(F.col("voltage_share_of_site_id"), F.col("voltage_cume_dist")).alias("greatest_voltage_share_of_site_id_voltage_cume_d"),
    )
    .orderBy(F.col("kwh_plus_voltage_cume_dist").asc())
)
