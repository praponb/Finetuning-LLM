from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_tariff_by_read_at_full = Window.partitionBy("site_tariff").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_tariff_by_read_at = Window.partitionBy("site_tariff").orderBy("read_at", "id")
w_all_by_site_commissioned_on = Window.orderBy(F.col("site_commissioned_on").desc(), "id")
w_site_tariff_site_commissione = Window.partitionBy("site_tariff", F.year("site_commissioned_on"))
w_site_commissioned_on_year = Window.partitionBy(F.year("site_commissioned_on"))
w_all_by_days_since_read_at_ru = Window.orderBy(F.col("days_since_read_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_id_rn_by_total_days_sin = Window.partitionBy("site_id", "rn").orderBy(F.col("total_days_since_read_at_share_of_site_tariff_si").desc(), "site_tariff", "site_id", "rn")
w_site_id_rn_by_total_days_sin_2 = Window.partitionBy("site_id", "rn").orderBy(F.col("total_days_since_read_at_share_of_site_tariff_si").desc())

base = (
    df_meter_readings
    .filter(F.col("quality") == "BAD")
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("read_at")).alias("days_since_read_at"),
    )
)

enriched = (
    base.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.quality",
        "s.days_since_read_at",
        "si.site_name",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.tariff").alias("site_tariff"),
    )
)

ranked = (
    enriched
    .select(
        "*",
        F.nth_value("read_at", 2).over(w_site_tariff_by_read_at_full).alias("second_read_at"),
        F.row_number().over(w_site_tariff_by_read_at).alias("rn"),
        F.ntile(4).over(w_all_by_site_commissioned_on).alias("site_commissioned_on_quartile"),
        F.round(F.col("days_since_read_at") / F.nullif(F.sum("days_since_read_at").over(w_site_tariff_site_commissione), F.lit(0)), 4).alias("days_since_read_at_share_of_site_tariff_site_com"),
        F.lead("site_commissioned_on").over(w_all_by_site_commissioned_on).alias("next_site_commissioned_on"),
        F.sum("days_since_read_at").over(w_site_commissioned_on_year).alias("site_commissioned_on_year_sum_days_since_read_at"),
        F.dense_rank().over(w_all_by_site_commissioned_on).alias("site_commissioned_on_dense_rank"),
        F.min("days_since_read_at").over(w_site_tariff_site_commissione).alias("site_tariff_site_commissioned_on_year_min_days_s"),
        F.round(F.col("days_since_read_at") / F.nullif(F.sum("days_since_read_at").over(w_site_commissioned_on_year), F.lit(0)), 4).alias("days_since_read_at_share_of_site_commissioned_on"),
        F.sum("days_since_read_at").over(w_all_by_days_since_read_at_ru).alias("running_days_since_read_at"),
        F.size(F.collect_set("site_tariff").over(w_site_commissioned_on_year)).alias("distinct_site_tariff_in_site_commissioned_on_yea"),
    )
)

labeled = (
    ranked
    .select(
        "*",
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
        F.greatest(F.col("days_since_read_at"), F.col("rn")).alias("greatest_days_since_read_at_rn"),
        F.greatest(F.col("rn"), F.col("days_since_read_at")).alias("greatest_rn_days_since_read_at"),
    )
)

totals = (
    labeled
    .groupBy("site_tariff", "site_id", "rn")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("days_since_read_at_share_of_site_tariff_site_com").alias("total_days_since_read_at_share_of_site_tariff_si"),
    )
    .filter(F.col("n_rows") >= 3)
)

windowed = (
    totals
    .select(
        "site_tariff",
        "site_id",
        "rn",
        "total_days_since_read_at_share_of_site_tariff_si",
        (F.col("n_rows") - F.lag("n_rows").over(w_site_id_rn_by_total_days_sin)).alias("n_rows_change"),
        F.round(F.percent_rank().over(w_site_id_rn_by_total_days_sin_2), 3).alias("total_days_since_read_at_share_of_site_tariff_si_2"),
    )
)

result = (
    windowed
    .select(
        "site_tariff",
        "rn",
        "total_days_since_read_at_share_of_site_tariff_si",
        "n_rows_change",
        "total_days_since_read_at_share_of_site_tariff_si_2",
        (F.col("n_rows_change") + F.col("rn")).alias("n_rows_change_plus_rn"),
        F.lower(F.regexp_replace(F.trim(F.col("site_tariff")), " ", "_")).alias("site_tariff_slug"),
    )
    .orderBy("n_rows_change", F.col("n_rows_change_plus_rn").asc_nulls_last())
)
