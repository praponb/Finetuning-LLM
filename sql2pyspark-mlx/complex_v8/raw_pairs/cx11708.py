from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("quality").orderBy(F.col("site_commissioned_on").desc(), F.col("id").desc())
w_read_at_dayofweek_quality = Window.partitionBy("read_at_dayofweek", "quality")
w_all_by_read_at_prior_6 = Window.orderBy(F.col("read_at").desc(), "id").rowsBetween(-6, -1)
w_all_by_read_at_moving_3 = Window.orderBy(F.col("read_at").desc(), "id").rowsBetween(-3, Window.currentRow)
w_site_id_read_at_dayofweek_by = Window.partitionBy("site_id", "read_at_dayofweek").orderBy(F.col("site_commissioned_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        F.dayofweek(F.col("read_at")).alias("read_at_dayofweek"),
    )
)

enriched_site = (
    prepared.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.length(F.trim(F.col("si.site_name"))) >= 7)
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.quality",
        "s.read_at_dayofweek",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        "si.site_name",
    )
)

latest_per_key = (
    enriched_site
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

joined_site = (
    latest_per_key.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.quality",
        "s.read_at_dayofweek",
        "s.site_commissioned_on",
        "s.site_name",
        F.col("si.zone").alias("site_zone"),
    )
)

with_windows = (
    joined_site
    .select(
        "*",
        F.count(F.lit(1)).over(w_read_at_dayofweek_quality).alias("rows_in_read_at_dayofweek_quality"),
        F.max("read_at_dayofweek").over(w_all_by_read_at_prior_6).alias("read_at_dayofweek_max_6_prior"),
        F.sum("kwh").over(w_all_by_read_at_moving_3).alias("kwh_sum_4"),
        F.first("read_at", ignorenulls=True).over(w_site_id_read_at_dayofweek_by).alias("first_known_read_at"),
    )
)

result = (
    with_windows
    .select(
        "read_at_dayofweek",
        "rows_in_read_at_dayofweek_quality",
        "first_known_read_at",
    )
    .distinct()
    .orderBy(
        "rows_in_read_at_dayofweek_quality",
        F.col("first_known_read_at").desc(),
    )
)
