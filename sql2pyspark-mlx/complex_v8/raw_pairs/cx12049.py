from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_by_days_since_read_a = Window.partitionBy("quality").orderBy(F.col("days_since_read_at").desc())
w_quality_by_days_since_read_a_2 = Window.partitionBy("quality").orderBy(F.col("days_since_read_at").desc(), "id").rowsBetween(-2, -1)
w_site_id_read_at_month_by_rea = Window.partitionBy("site_id", F.date_trunc("month", "read_at")).orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_id = Window.partitionBy("site_id")
w_site_id_read_at_month_by_rea_2 = Window.partitionBy("site_id", F.date_trunc("month", "read_at")).orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_meter_readings
    .filter((F.col("read_at") >= F.lit("2025-01-25 11:00:00").cast("timestamp")) & (F.col("voltage") <= 240.9))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
        F.concat(F.col("quality"), F.lit("#"), F.col("site_id").cast("string")).alias("quality_ref"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("read_at")).alias("days_since_read_at"),
    )
)

with_activity = (
    cleaned
    .crossJoin(cleaned.agg(F.avg("kwh").alias("overall_avg_kwh")))
    .filter(F.col("kwh") > F.col("overall_avg_kwh"))
    .drop("overall_avg_kwh")
)

ranked = (
    with_activity
    .select(
        "*",
        F.rank().over(w_quality_by_days_since_read_a).alias("days_since_read_at_rank"),
        F.sum("kwh").over(w_quality_by_days_since_read_a_2).alias("kwh_sum_2_prior"),
        F.last("voltage").over(w_site_id_read_at_month_by_rea).alias("last_voltage"),
        F.sum("voltage").over(w_site_id).alias("site_id_sum_voltage"),
        F.round(F.col("days_since_read_at") / F.nullif(F.sum("days_since_read_at").over(w_site_id), F.lit(0)), 4).alias("days_since_read_at_share_of_site_id"),
        F.sum("days_since_read_at").over(w_site_id_read_at_month_by_rea_2).alias("running_days_since_read_at"),
    )
)

enriched = (
    ranked.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.quality",
        "s.quality_ref",
        "s.days_since_read_at",
        "s.days_since_read_at_rank",
        "s.kwh_sum_2_prior",
        "s.last_voltage",
        "s.site_id_sum_voltage",
        "s.days_since_read_at_share_of_site_id",
        "s.running_days_since_read_at",
        F.col("si.zone").alias("site_zone"),
    )
)

result = (
    enriched
    .groupingSets(
        [["site_zone", "days_since_read_at_rank"], ["site_zone"]],
        "site_zone", "days_since_read_at_rank",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.max("read_at").alias("last_read_at"),
    )
    .orderBy(
        F.col("site_zone").asc_nulls_last(),
        F.col("days_since_read_at_rank").asc_nulls_last(),
    )
)
