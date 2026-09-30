from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_year = Window.partitionBy(F.year("read_at"))
w_all_by_kwh = Window.orderBy("kwh", "id")
w_all_by_read_at_running = Window.orderBy(F.col("read_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_read_at_year_by_read_at = Window.partitionBy(F.year("read_at")).orderBy(F.col("read_at").desc(), "id")
w_all_by_read_at = Window.orderBy(F.col("read_at").asc_nulls_last(), "id")
w_read_at_year_by_read_at_movi = Window.partitionBy(F.year("read_at")).orderBy(F.col("read_at").desc(), "id").rowsBetween(-6, Window.currentRow)

prepared = (
    df_meter_readings
    .filter((F.col("quality") == "BAD") & (F.col("site_id").isNotNull()))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

joined = (
    prepared.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.site_name").isNotNull()), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.quality",
        F.col("si.tariff").alias("site_tariff"),
    )
)

sequenced = (
    joined
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "site_tariff",
        F.size(F.collect_set("quality").over(w_read_at_year)).alias("distinct_quality_in_read_at_year"),
        F.rank().over(w_all_by_kwh).alias("kwh_rank"),
        F.first("kwh", ignorenulls=True).over(w_all_by_read_at_running).alias("first_known_kwh"),
        F.round(F.percent_rank().over(w_read_at_year_by_read_at), 3).alias("read_at_percent_rank"),
        F.lag("kwh").over(w_all_by_kwh).alias("prev_kwh"),
        F.round(F.col("kwh") / F.nullif(F.sum("kwh").over(w_read_at_year), F.lit(0)), 4).alias("kwh_share_of_read_at_year"),
        F.size(F.collect_set("site_id").over(w_read_at_year)).alias("distinct_site_id_in_read_at_year"),
        F.count(F.lit(1)).over(w_read_at_year).alias("rows_in_read_at_year"),
        F.round((F.unix_timestamp("read_at") - F.unix_timestamp(F.lag("read_at").over(w_all_by_read_at))) / 60, 1).alias("minutes_since_prev_read_at"),
        F.ntile(10).over(w_read_at_year_by_read_at).alias("read_at_decile"),
        F.size(F.collect_set("site_tariff").over(w_read_at_year)).alias("distinct_site_tariff_in_read_at_year"),
        F.round(F.col("kwh") - F.avg("kwh").over(w_read_at_year), 2).alias("kwh_vs_read_at_year_avg"),
        F.lead("read_at").over(w_all_by_kwh).alias("next_read_at"),
        F.round(F.avg("kwh").over(w_read_at_year), 2).alias("read_at_year_avg_kwh"),
        F.max("kwh").over(w_read_at_year_by_read_at_movi).alias("kwh_max_7"),
    )
)

first_rows = (
    sequenced
    .filter((F.col("kwh_rank") <= 2) & (F.col("distinct_site_tariff_in_read_at_year").between(4, 9)))
)

result = (
    first_rows
    .select(
        "id",
        "site_id",
        "kwh",
        "site_tariff",
        "distinct_quality_in_read_at_year",
        "kwh_rank",
        "read_at_percent_rank",
        "prev_kwh",
        "kwh_share_of_read_at_year",
        "distinct_site_id_in_read_at_year",
        "rows_in_read_at_year",
        "minutes_since_prev_read_at",
        "kwh_vs_read_at_year_avg",
        "next_read_at",
        "read_at_year_avg_kwh",
        "kwh_max_7",
        F.when(F.col("site_tariff").isin("TOU", "FLAT"), "group_a").when(F.col("site_tariff") == "EV", "group_b").otherwise("other").alias("site_tariff_group"),
        F.concat(F.col("site_tariff"), F.lit("#"), F.col("read_at_decile").cast("string")).alias("site_tariff_ref"),
    )
    .orderBy(F.col("kwh_max_7").asc(), F.col("site_tariff_group").desc())
)
