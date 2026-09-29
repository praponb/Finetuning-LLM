from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_month = Window.partitionBy(F.date_trunc("month", "read_at"))
w_quality_read_at_to_date_by_k = Window.partitionBy("quality", F.to_date("read_at")).orderBy("kwh_bucket", "id")
w_site_id_by_voltage = Window.partitionBy("site_id").orderBy(F.col("voltage").desc(), "id")

prepared_readings = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        (F.floor(F.col("kwh") / 25) * 25).alias("kwh_bucket"),
    )
)

derived = (
    prepared_readings
    .filter(~F.col("quality").isin("BAD", "EST"))
    .withColumn("voltage_distance", F.abs(F.col("voltage") - 225.3))
)

metrics = (
    derived
    .select(
        "*",
        F.round(F.col("voltage_distance") - F.avg("voltage_distance").over(w_read_at_month), 2).alias("voltage_distance_vs_read_at_month_avg"),
        F.round(F.col("voltage") / F.nullif(F.sum("voltage").over(w_read_at_month), F.lit(0)), 4).alias("voltage_share_of_read_at_month"),
        F.row_number().over(w_quality_read_at_to_date_by_k).alias("rn"),
        F.row_number().over(w_site_id_by_voltage).alias("rn_2"),
    )
)

result = (
    metrics
    .groupingSets(
        [["quality", "rn", "site_id"], ["quality"]],
        "quality", "rn", "site_id",
    )
    .agg(
        F.count("*").alias("n_rows"),
        F.sum(F.when(F.col("quality").isin("EST", "OK"), F.col("voltage_share_of_read_at_month")).otherwise(0)).alias("voltage_share_of_read_at_month_quality_in_est_ok"),
    )
    .orderBy(
        F.col("quality").asc_nulls_last(),
        F.col("rn").asc_nulls_last(),
        F.col("site_id").asc_nulls_last(),
    )
)
