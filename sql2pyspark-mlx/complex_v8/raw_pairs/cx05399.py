from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_reading_quality_by_kwh_adj_t = Window.partitionBy("reading_quality").orderBy(F.col("kwh_adj").desc_nulls_first())
w_reading_quality_by_kwh_adj = Window.partitionBy("reading_quality").orderBy(F.col("kwh_adj").desc_nulls_first(), "id", "reading_id")
w_reading_quality = Window.partitionBy("reading_quality")
w_reading_quality_by_kwh_adj_f = Window.partitionBy("reading_quality").orderBy(F.col("kwh_adj").desc_nulls_first(), "id", "reading_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

base = (
    df_meter_readings
    .filter((F.col("kwh") <= 83.8) & (F.col("site_id").isNotNull()))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.greatest(F.col("kwh"), F.col("voltage")).alias("greatest_kwh_voltage"),
        F.length(F.trim(F.col("quality"))).alias("quality_len"),
        F.round(F.col("kwh") * 0.25, 2).alias("kwh_adj"),
    )
)

with_children_readings = (
    base.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.voltage",
        "s.greatest_kwh_voltage",
        "s.kwh_adj",
        F.col("m.id").alias("reading_id"),
        F.col("m.quality").alias("reading_quality"),
        F.col("m.voltage").alias("reading_voltage"),
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.kwh").alias("reading_kwh"),
    )
)

with_parent = (
    with_children_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .join(df_meter_readings.alias("m"), F.col("m.id") == F.col("s.reading_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh_adj",
        "s.reading_id",
        "s.reading_quality",
        "s.reading_read_at",
        "s.reading_kwh",
        F.col("si.installed_kw").alias("site_installed_kw"),
        F.col("m.read_at").alias("reading_read_at_2"),
        F.col("m.kwh").alias("reading_kwh_2"),
    )
)

with_trends = (
    with_parent
    .select(
        "*",
        F.dense_rank().over(w_reading_quality_by_kwh_adj_t).alias("kwh_adj_dense_rank"),
        F.ntile(5).over(w_reading_quality_by_kwh_adj).alias("kwh_adj_quintile"),
        F.round(F.col("reading_kwh_2") / F.nullif(F.sum("reading_kwh_2").over(w_reading_quality), F.lit(0)), 4).alias("reading_kwh_2_share_of_reading_quality"),
        F.nth_value("reading_kwh_2", 2).over(w_reading_quality_by_kwh_adj_f).alias("second_reading_kwh_2"),
        F.lead("reading_kwh").over(w_reading_quality_by_kwh_adj).alias("next_reading_kwh"),
        F.rank().over(w_reading_quality_by_kwh_adj_t).alias("kwh_adj_rank"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "reading_id",
        F.col("kwh_adj_quintile").alias("kwh_adj_quintile_v"),
        F.col("kwh_adj_dense_rank").alias("kwh_adj_dense_rank_v"),
    )
    .unpivot(
        ids=["id", "reading_id"],
        values=["kwh_adj_quintile_v", "kwh_adj_dense_rank_v"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
    .orderBy("id", "reading_id", "metric_name")
)
