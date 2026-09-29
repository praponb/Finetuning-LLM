from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_hour = Window.partitionBy("read_at_hour")
w_site_id_by_read_at_hour_ties = Window.partitionBy("site_id").orderBy("read_at_hour")
w_site_id_by_read_at_hour_full = Window.partitionBy("site_id").orderBy(F.col("read_at_hour").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_id_by_read_at_hour_prio = Window.partitionBy("site_id").orderBy("read_at_hour", "id").rowsBetween(-6, -1)
w_site_id_by_read_at_hour_movi = Window.partitionBy("site_id").orderBy(F.col("read_at_hour").desc(), "id").rowsBetween(-3, Window.currentRow)

filtered = (
    df_meter_readings
    .filter((F.col("kwh").between(15.9, 85.7)) | (F.col("site_id") <= 2))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.hour(F.col("read_at")).alias("read_at_hour"),
        F.round(F.col("voltage") / F.nullif(F.col("kwh"), F.lit(0)), 3).alias("voltage_per_kwh"),
    )
)

ranked = (
    filtered
    .select(
        "*",
        F.size(F.collect_set("quality").over(w_read_at_hour)).alias("distinct_quality_in_read_at_hour"),
        F.max("voltage_per_kwh").over(w_read_at_hour).alias("read_at_hour_max_voltage_per_kwh"),
        F.sum("kwh").over(w_read_at_hour).alias("read_at_hour_sum_kwh"),
        F.rank().over(w_site_id_by_read_at_hour_ties).alias("read_at_hour_rank"),
        F.last("voltage_per_kwh").over(w_site_id_by_read_at_hour_full).alias("last_voltage_per_kwh"),
        F.max("read_at_hour").over(w_site_id_by_read_at_hour_prio).alias("read_at_hour_max_6_prior"),
        F.round(F.cume_dist().over(w_site_id_by_read_at_hour_ties), 3).alias("read_at_hour_cume_dist"),
        F.sum("read_at_hour").over(w_site_id_by_read_at_hour_movi).alias("read_at_hour_sum_4"),
    )
)

first_rows = (
    ranked
    .filter(F.col("read_at_hour_rank") == 1)
    .drop("read_at_hour_rank")
)

result = (
    first_rows
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "read_at_hour",
        "distinct_quality_in_read_at_hour",
        "read_at_hour_max_voltage_per_kwh",
        "read_at_hour_sum_kwh",
        "last_voltage_per_kwh",
        "read_at_hour_cume_dist",
        "read_at_hour_sum_4",
        F.coalesce(F.col("last_voltage_per_kwh"), F.lit(0.0)).alias("last_voltage_per_kwh_filled"),
    )
    .orderBy(
        F.col("read_at_hour_cume_dist").desc(),
        "distinct_quality_in_read_at_hour",
    )
)
