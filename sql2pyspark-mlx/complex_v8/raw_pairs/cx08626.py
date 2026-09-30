from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_site_commissioned_on = Window.orderBy("site_commissioned_on", "id")
w_site_id = Window.partitionBy("site_id")
w_all_by_site_commissioned_on_2 = Window.orderBy("site_commissioned_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_id_site_zone_by_read_at = Window.partitionBy("site_id", "site_zone").orderBy(F.col("read_at").desc_nulls_first(), "id")
w_all_by_kwh_adj_moving_3 = Window.orderBy(F.col("kwh_adj").desc_nulls_first(), "id").rowsBetween(-3, Window.currentRow)
w_all_by_kwh_adj_running = Window.orderBy(F.col("kwh_adj").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_zone = Window.partitionBy("site_zone")
w_read_at_plus_2h_year_read_at = Window.partitionBy(F.year("read_at_plus_2h"), F.year("read_at")).orderBy("next_kwh_adj", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_read_at_rank_by_next_kwh_adj = Window.partitionBy("read_at_rank").orderBy("next_kwh_adj", "id")
w_all_by_prev_kwh = Window.orderBy(F.col("prev_kwh").desc(), "id")

prepared = (
    df_meter_readings
    .filter((F.to_date(F.col("read_at")) < F.lit("2025-03-02").cast("date")) & (F.col("voltage") > 220.7))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.round(F.col("kwh") * 1.5, 2).alias("kwh_adj"),
        (F.col("read_at") + F.expr("INTERVAL 2 HOURS")).alias("read_at_plus_2h"),
    )
)

with_parent_site = (
    prepared.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.*",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.zone").alias("site_zone"),
        "si.site_name",
    )
)

ranked = (
    with_parent_site
    .select(
        "*",
        F.lead("kwh_adj").over(w_all_by_site_commissioned_on).alias("next_kwh_adj"),
        (F.col("kwh") - F.lag("kwh").over(w_all_by_site_commissioned_on)).alias("kwh_change"),
        F.round(F.col("kwh_adj") - F.avg("kwh_adj").over(w_site_id), 2).alias("kwh_adj_vs_site_id_avg"),
        F.round(F.col("kwh_adj") / F.nullif(F.sum("kwh_adj").over(w_site_id), F.lit(0)), 4).alias("kwh_adj_share_of_site_id"),
        F.sum("voltage").over(w_all_by_site_commissioned_on_2).alias("running_voltage"),
        F.lag("kwh", 1, 0).over(w_site_id_site_zone_by_read_at).alias("prev_kwh"),
        F.max("kwh_adj").over(w_all_by_kwh_adj_moving_3).alias("kwh_adj_max_4"),
        F.rank().over(w_site_id_site_zone_by_read_at).alias("read_at_rank"),
        F.count(F.lit(1)).over(w_all_by_kwh_adj_running).alias("running_count"),
        F.row_number().over(w_site_id_site_zone_by_read_at).alias("read_at_seq"),
    )
)

first_rows = (
    ranked
    .filter(F.col("read_at_seq") <= 5)
    .drop("read_at_seq")
)

sequenced = (
    first_rows
    .select(
        "id",
        "site_id",
        "voltage",
        "kwh_adj",
        "read_at_plus_2h",
        "site_zone",
        "site_name",
        "next_kwh_adj",
        "kwh_change",
        "kwh_adj_vs_site_id_avg",
        "kwh_adj_share_of_site_id",
        "running_voltage",
        "prev_kwh",
        "kwh_adj_max_4",
        "read_at_rank",
        "running_count",
        F.max("kwh_change").over(w_site_zone).alias("site_zone_max_kwh_change"),
        F.first("site_commissioned_on", ignorenulls=True).over(w_read_at_plus_2h_year_read_at).alias("first_known_site_commissioned_on"),
        F.lag("kwh_change", 3).over(w_read_at_rank_by_next_kwh_adj).alias("prev3_kwh_change"),
        F.row_number().over(w_all_by_prev_kwh).alias("seq_no"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "site_id",
        "read_at_plus_2h",
        "site_zone",
        "site_name",
        "kwh_change",
        "kwh_adj_vs_site_id_avg",
        "prev_kwh",
        "kwh_adj_max_4",
        "read_at_rank",
        "running_count",
        "site_zone_max_kwh_change",
        "prev3_kwh_change",
        "seq_no",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("read_at_plus_2h")).alias("days_since_read_at_plus_2h"),
        F.concat(F.col("site_zone"), F.lit("#"), F.col("seq_no").cast("string")).alias("site_zone_ref"),
    )
    .orderBy("site_zone_max_kwh_change", F.col("id").desc_nulls_first())
)
