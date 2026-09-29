from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_by_voltage_ties = Window.partitionBy("quality").orderBy("voltage")
w_quality_by_voltage = Window.partitionBy("quality").orderBy("voltage", "id")
w_quality_by_voltage_full = Window.partitionBy("quality").orderBy("voltage", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_id_read_at_to_date = Window.partitionBy("site_id", F.to_date("read_at"))

prepared_readings = (
    df_meter_readings
    .filter((F.col("kwh") < 84.9) & (F.month(F.col("read_at")).isin(1, 3)))
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        F.upper(F.trim(F.col("quality"))).alias("quality_clean"),
        (F.col("kwh") + F.col("voltage")).alias("kwh_plus_voltage"),
    )
)

screened = (
    prepared_readings.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.kwh") <= 87.1) & (F.col("m.voltage").between(219.5, 240.1)), "left_anti")
)

with_windows = (
    screened
    .select(
        "*",
        F.round(F.percent_rank().over(w_quality_by_voltage_ties), 3).alias("voltage_percent_rank"),
        F.round(F.cume_dist().over(w_quality_by_voltage), 3).alias("voltage_cume_dist"),
        F.last("voltage").over(w_quality_by_voltage_full).alias("last_voltage"),
        F.first("read_at").over(w_quality_by_voltage_full).alias("first_read_at"),
        F.size(F.collect_set("quality").over(w_site_id_read_at_to_date)).alias("distinct_quality_in_site_id_read_at_to_date"),
        F.dense_rank().over(w_quality_by_voltage).alias("voltage_dense_rank"),
    )
)

result = (
    with_windows
    .cube("site_id", "voltage_dense_rank")
    .agg(F.min("read_at").alias("first_read_at_2"), F.count("*").alias("n_rows"))
    .orderBy(
        F.col("site_id").asc_nulls_last(),
        F.col("voltage_dense_rank").asc_nulls_last(),
    )
)
