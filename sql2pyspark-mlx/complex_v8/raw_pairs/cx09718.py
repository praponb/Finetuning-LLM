from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_reading_quality_group_readin = Window.partitionBy("reading_quality_group", "reading_id")
w_all_by_kwh_adj_ties = Window.orderBy(F.col("kwh_adj").desc())
w_all_by_kwh_adj = Window.orderBy(F.col("kwh_adj").desc(), "id", "reading_id")
w_reading_quality_group = Window.partitionBy("reading_quality_group")
w_reading_quality_group_readin_2 = Window.partitionBy("reading_quality_group", "reading_id").orderBy("next3_kwh_adj", "id", "reading_id")

valid = (
    df_meter_readings
    .filter(F.col("site_id") >= 2)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.round(F.col("kwh") * 1.1, 2).alias("kwh_adj"),
        F.round(F.col("voltage") / F.nullif(F.col("kwh"), F.lit(0)), 3).alias("voltage_per_kwh"),
        F.substring(F.trim(F.col("quality")), 1, 3).alias("quality_prefix"),
    )
)

exploded_detail_readings = (
    valid.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.quality",
        "s.kwh_adj",
        F.col("m.id").alias("reading_id"),
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.quality").alias("reading_quality"),
        F.col("m.kwh").alias("reading_kwh"),
    )
)

scored = (
    exploded_detail_readings
    .select(
        "id",
        "site_id",
        "kwh_adj",
        "reading_id",
        "reading_quality",
        "reading_kwh",
        F.date_format(F.col("reading_read_at"), "MMM yyyy").alias("reading_read_at_label"),
        F.when(F.col("reading_quality").isin("BAD", "EST"), "group_a").when(F.col("reading_quality") == "OK", "group_b").otherwise("other").alias("reading_quality_group"),
    )
)

metrics = (
    scored
    .select(
        "*",
        F.min("reading_kwh").over(w_reading_quality_group_readin).alias("reading_quality_group_reading_id_min_reading_kwh"),
        F.round(F.percent_rank().over(w_all_by_kwh_adj_ties), 3).alias("kwh_adj_percent_rank"),
        F.lead("kwh_adj", 3).over(w_all_by_kwh_adj).alias("next3_kwh_adj"),
        F.round(F.col("kwh_adj") - F.avg("kwh_adj").over(w_reading_quality_group_readin), 2).alias("kwh_adj_vs_reading_quality_group_reading_id_avg"),
        F.count(F.lit(1)).over(w_reading_quality_group_readin).alias("rows_in_reading_quality_group_reading_id"),
    )
)

ranked = (
    metrics
    .select(
        "id",
        "kwh_adj",
        "reading_id",
        "reading_quality",
        "reading_read_at_label",
        "kwh_adj_percent_rank",
        "next3_kwh_adj",
        "kwh_adj_vs_reading_quality_group_reading_id_avg",
        F.round(F.avg("kwh_adj").over(w_reading_quality_group), 2).alias("reading_quality_group_avg_kwh_adj"),
        F.dense_rank().over(w_reading_quality_group_readin_2).alias("next3_kwh_adj_dense_rank"),
        F.round(F.col("kwh_adj") / F.nullif(F.sum("kwh_adj").over(w_reading_quality_group), F.lit(0)), 4).alias("kwh_adj_share_of_reading_quality_group"),
        F.count(F.lit(1)).over(w_reading_quality_group).alias("rows_in_reading_quality_group"),
    )
)

result = (
    ranked
    .groupBy("reading_id")
    .pivot("reading_quality", ["BAD", "EST", "OK"])
    .agg(F.sum("rows_in_reading_quality_group"))
)
