from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_site_commissioned_on = Window.orderBy(F.col("site_commissioned_on_plus_7d").desc())
w_all_by_site_commissioned_on_2 = Window.orderBy(F.col("site_commissioned_on_plus_7d").asc_nulls_last(), "id")

base = (
    df_meter_readings
    .filter((F.col("kwh").between(37.1, 85.7)) & (F.hour(F.col("read_at")).between(6, 8)))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        F.substring(F.trim(F.col("quality")), 1, 3).alias("quality_prefix"),
    )
)

joined = (
    base.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.quality_prefix",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

enriched = (
    joined
    .select(
        "id",
        F.date_add(F.col("site_commissioned_on"), 7).alias("site_commissioned_on_plus_7d"),
    )
)

sequenced = (
    enriched
    .select(
        "*",
        F.rank().over(w_all_by_site_commissioned_on).alias("site_commissioned_on_plus_7d_rank"),
        F.ntile(5).over(w_all_by_site_commissioned_on_2).alias("site_commissioned_on_plus_7d_quintile"),
        F.round(F.cume_dist().over(w_all_by_site_commissioned_on), 3).alias("site_commissioned_on_plus_7d_cume_dist"),
    )
)

result = (
    sequenced
    .groupingSets(
        [["site_commissioned_on_plus_7d_quintile", "site_commissioned_on_plus_7d_rank"], ["site_commissioned_on_plus_7d_quintile"]],
        "site_commissioned_on_plus_7d_quintile", "site_commissioned_on_plus_7d_rank",
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.stddev("site_commissioned_on_plus_7d_cume_dist"), 2).alias("stddev_site_commissioned_on_plus_7d_cume_dist"),
    )
    .orderBy(
        F.col("site_commissioned_on_plus_7d_quintile").asc_nulls_last(),
        F.col("site_commissioned_on_plus_7d_rank").asc_nulls_last(),
    )
)
