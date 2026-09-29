from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_kwh_tier = Window.partitionBy("site_id", "kwh_tier")
w_kwh_tier_by_read_at_full = Window.partitionBy("kwh_tier").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped_readings = (
    df_meter_readings
    .filter(F.col("read_at") < F.lit("2025-02-12 08:00:00").cast("timestamp"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.coalesce(F.col("voltage"), F.lit(0.0)).alias("voltage_filled"),
        F.when(F.col("kwh") >= 104.8, "A").when(F.col("kwh") >= 30.2, "B").otherwise("C").alias("kwh_tier"),
        F.date_format(F.col("read_at"), "yyyy-MM-dd HH:00").alias("read_at_label"),
    )
)

ranked = (
    scoped_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        "voltage_filled",
        "kwh_tier",
        F.count(F.lit(1)).over(w_site_id_kwh_tier).alias("rows_in_site_id_kwh_tier"),
        F.first("voltage").over(w_kwh_tier_by_read_at_full).alias("first_voltage"),
    )
)

result = (
    ranked
    .groupingSets(
        [["quality", "site_id", "kwh_tier"], ["quality"]],
        "quality", "site_id", "kwh_tier",
    )
    .agg(F.count("*").alias("n_rows"))
    .select(
        F.coalesce(F.col("quality"), F.lit("ALL")).alias("quality"),
        "site_id",
        F.coalesce(F.col("kwh_tier"), F.lit("ALL")).alias("kwh_tier"),
        "n_rows",
    )
    .orderBy(
        F.col("quality").asc_nulls_last(),
        F.col("site_id").asc_nulls_last(),
        F.col("kwh_tier").asc_nulls_last(),
    )
)
