from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_read_at_to_date_by_v = Window.partitionBy("site_id", F.to_date("read_at")).orderBy("voltage", "id").rowsBetween(-1, -1)
w_site_id_read_at_to_date_by_v_2 = Window.partitionBy("site_id", F.to_date("read_at")).orderBy("voltage")

recent_readings = (
    df_meter_readings
    .filter(F.col("site_id").isNotNull())
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.when(F.col("quality").isin("BAD", "OK"), "group_a").when(F.col("quality") == "EST", "group_b").otherwise("other").alias("quality_group"),
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
        F.upper(F.trim(F.col("quality"))).alias("quality_clean"),
    )
)

ranked = (
    recent_readings
    .select(
        "*",
        F.round(F.avg("kwh").over(w_site_id_read_at_to_date_by_v), 2).alias("kwh_avg_1_prior"),
        F.rank().over(w_site_id_read_at_to_date_by_v_2).alias("voltage_rank"),
    )
)

filtered = (
    ranked.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.commissioned_on") < F.lit("2025-11-28").cast("date")), "left_semi")
)

result = (
    filtered
    .select(
        "id",
        "site_id",
        "voltage",
        "quality_group",
        "quality_slug",
        "quality_clean",
        "kwh_avg_1_prior",
        "voltage_rank",
    )
)
