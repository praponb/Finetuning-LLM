from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_quarter_by_site_comm = Window.partitionBy("read_at_quarter").orderBy(F.col("site_commissioned_on").desc())
w_quality = Window.partitionBy("quality")
w_read_at_quarter_by_read_at_q = Window.partitionBy("read_at_quarter").orderBy("read_at_quarter", "id")
w_read_at_quarter_by_read_at_q_2 = Window.partitionBy("read_at_quarter").orderBy("read_at_quarter", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_read_at_quarter_by_read_at_q_3 = Window.partitionBy("read_at_quarter").orderBy("read_at_quarter", "id").rowsBetween(-1, -1)
w_all_by_kwh_prior_6 = Window.orderBy(F.col("kwh").desc(), "id").rowsBetween(-6, -1)
w_last_known_read_at_quarter_s = Window.partitionBy("last_known_read_at_quarter", F.year("site_commissioned_on"))
w_site_id_by_rows_in_quality_f = Window.partitionBy("site_id").orderBy("rows_in_quality", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

recent_readings = (
    df_meter_readings
    .filter(F.col("voltage") >= 222.3)
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        F.quarter(F.col("read_at")).alias("read_at_quarter"),
    )
)

enriched = (
    recent_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.quality",
        "s.read_at_quarter",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

windowed = (
    enriched
    .select(
        "id",
        "site_id",
        "quality",
        "site_commissioned_on",
        F.round(F.cume_dist().over(w_read_at_quarter_by_site_comm), 3).alias("site_commissioned_on_cume_dist"),
        F.round(F.col("read_at_quarter") / F.nullif(F.sum("read_at_quarter").over(w_quality), F.lit(0)), 4).alias("read_at_quarter_share_of_quality"),
        F.rank().over(w_read_at_quarter_by_site_comm).alias("site_commissioned_on_rank"),
        F.lead("read_at_quarter", 2).over(w_read_at_quarter_by_read_at_q).alias("next2_read_at_quarter"),
        F.round(F.col("kwh") - F.avg("kwh").over(w_quality), 2).alias("kwh_vs_quality_avg"),
        F.last("read_at_quarter", ignorenulls=True).over(w_read_at_quarter_by_read_at_q_2).alias("last_known_read_at_quarter"),
        F.round(F.avg("kwh").over(w_read_at_quarter_by_read_at_q_3), 2).alias("kwh_avg_1_prior"),
        F.count(F.lit(1)).over(w_quality).alias("rows_in_quality"),
        F.lead("read_at_quarter", 1, 0).over(w_read_at_quarter_by_read_at_q).alias("next_read_at_quarter"),
        F.max("kwh").over(w_all_by_kwh_prior_6).alias("kwh_max_6_prior"),
        F.lead("read_at_quarter").over(w_read_at_quarter_by_read_at_q).alias("next_read_at_quarter_2"),
    )
)

sequenced = (
    windowed
    .select(
        "id",
        "site_id",
        "site_commissioned_on",
        "site_commissioned_on_cume_dist",
        "read_at_quarter_share_of_quality",
        "site_commissioned_on_rank",
        "last_known_read_at_quarter",
        "rows_in_quality",
        "next_read_at_quarter",
        "kwh_max_6_prior",
        "next_read_at_quarter_2",
        F.size(F.collect_set("site_id").over(w_last_known_read_at_quarter_s)).alias("distinct_site_id_in_last_known_read_at_quarter_s"),
        F.nth_value("site_commissioned_on", 2).over(w_site_id_by_rows_in_quality_f).alias("second_site_commissioned_on"),
        F.sum("next_read_at_quarter_2").over(w_last_known_read_at_quarter_s).alias("last_known_read_at_quarter_site_commissioned_on_"),
    )
)

per_group = (
    sequenced
    .groupBy(
        F.date_format("site_commissioned_on", "yyyy-MM").alias("site_commissioned_on_ym"),
        "site_commissioned_on_rank",
    )
    .agg(
        F.min("site_commissioned_on").alias("first_site_commissioned_on"),
        F.countDistinct("site_id").alias("distinct_site_ids"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("cnt") >= 3)
)

result = (
    per_group
    .select(
        "site_commissioned_on_ym",
        "site_commissioned_on_rank",
        "first_site_commissioned_on",
        "distinct_site_ids",
        "cnt",
    )
    .orderBy(F.col("cnt").desc())
)
