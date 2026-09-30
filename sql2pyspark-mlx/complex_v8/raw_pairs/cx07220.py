from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("zone").orderBy(F.col("rate").desc(), F.col("id").desc())
w_all_by_rate = Window.orderBy("rate", "id")
w_zone_by_rate = Window.partitionBy("zone").orderBy(F.col("rate").asc_nulls_last(), "id")
w_site_id = Window.partitionBy("site_id")
w_rate_rank_site_id_by_site_id = Window.partitionBy("rate_rank", "site_id").orderBy(F.col("site_id_avg_voltage").desc_nulls_first(), "id").rowsBetween(-3, Window.currentRow)
w_all_by_rate_rank = Window.orderBy("rate_rank", "id")
w_rate_rank_zone = Window.partitionBy("rate_rank", "zone")
w_site_id_zone_by_rate_quartil = Window.partitionBy("site_id", "zone").orderBy("rate_quartile", "id")

joined_ranges = (
    df_meter_readings.alias("r")
    .join(df_sites.alias("st"), F.col("st.id") == F.col("r.site_id"), "inner")
    .join(
        df_tariff_windows.alias("tw"),
        (F.col("tw.tariff") == F.col("st.tariff"))
        & (F.col("r.read_at") >= F.col("tw.valid_from"))
        & (F.col("r.read_at") < F.col("tw.valid_to")),
        "left",
    )
    .select(
        "r.id",
        "r.site_id",
        "r.read_at",
        "r.kwh",
        "r.voltage",
        "st.zone",
        "tw.rate",
        F.round(F.col("r.kwh") * F.col("tw.rate"), 2).alias("energy_cost"),
    )
)

unique_rows = (
    joined_ranges
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

sequenced = (
    unique_rows
    .select(
        "id",
        "site_id",
        "read_at",
        "zone",
        "rate",
        F.rank().over(w_all_by_rate).alias("rate_rank"),
        F.ntile(4).over(w_zone_by_rate).alias("rate_quartile"),
        F.round(F.col("kwh") - F.avg("kwh").over(w_site_id), 2).alias("kwh_vs_site_id_avg"),
        F.min("energy_cost").over(w_site_id).alias("site_id_min_energy_cost"),
        F.round(F.avg("voltage").over(w_site_id), 2).alias("site_id_avg_voltage"),
    )
)

metrics = (
    sequenced
    .select(
        "id",
        "site_id",
        "read_at",
        "zone",
        "rate",
        "rate_rank",
        "rate_quartile",
        "kwh_vs_site_id_avg",
        "site_id_min_energy_cost",
        F.round(F.avg("rate").over(w_rate_rank_site_id_by_site_id), 2).alias("rate_avg_4"),
        F.lead("rate", 3).over(w_all_by_rate_rank).alias("next3_rate"),
        F.count(F.lit(1)).over(w_rate_rank_zone).alias("rows_in_rate_rank_zone"),
        F.lead("read_at").over(w_site_id_zone_by_rate_quartil).alias("next_read_at"),
        F.round(F.avg("rate_quartile").over(w_rate_rank_zone), 2).alias("rate_rank_zone_avg_rate_quartile"),
    )
)

result = (
    metrics
    .select(
        "id",
        "site_id",
        "read_at",
        "zone",
        "rate",
        "rate_rank",
        "rate_quartile",
        "kwh_vs_site_id_avg",
        "site_id_min_energy_cost",
        "rate_avg_4",
        "next3_rate",
        "rows_in_rate_rank_zone",
        "rate_rank_zone_avg_rate_quartile",
        F.round(F.col("rate_avg_4") * 1.5, 2).alias("rate_avg_4_adj"),
    )
)
