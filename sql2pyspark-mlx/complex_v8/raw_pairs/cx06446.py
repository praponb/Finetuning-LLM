from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id")

prepared = (
    df_meter_readings
    .filter((F.col("kwh") > 20.4) & (F.col("voltage") <= 240.2))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

tagged = (
    prepared
    .filter(F.col("voltage") <= 232.4)
    .select(
        "id",
        "site_id",
        "kwh",
        F.hour(F.col("read_at")).alias("read_at_hour"),
    )
)

with_windows = (
    tagged
    .withColumn("rows_in_site_id", F.count(F.lit(1)).over(w_site_id))
    .withColumn("site_id_sum_read_at_hour", F.sum("read_at_hour").over(w_site_id))
)

result = (
    with_windows
    .rollup("site_id", "read_at_hour")
    .agg(
        F.percentile_approx("site_id_sum_read_at_hour", 0.5).alias("median_site_id_sum_read_at_hour"),
        F.round(F.avg("site_id_sum_read_at_hour"), 2).alias("avg_site_id_sum_read_at_hour"),
    )
    .orderBy(
        F.col("site_id").asc_nulls_last(),
        F.col("read_at_hour").asc_nulls_last(),
    )
)
