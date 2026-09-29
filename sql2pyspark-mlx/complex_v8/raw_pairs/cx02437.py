from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id")
w_read_at_year_site_id_by_rate = Window.partitionBy(F.year("read_at"), "site_id").orderBy("rate", "id")
w_read_at_year_site_id_by_rate_2 = Window.partitionBy(F.year("read_at"), "site_id").orderBy("rate", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_read_at_year_site_id_by_rate_3 = Window.partitionBy(F.year("read_at"), "site_id").orderBy("rate")

joined_ranges = (
    df_meter_readings.alias("r")
    .join(df_sites.alias("st"), F.col("st.id") == F.col("r.site_id"), "inner")
    .join(
        df_tariff_windows.alias("tw"),
        (F.col("tw.tariff") == F.col("st.tariff"))
        & (F.col("r.read_at") >= F.col("tw.valid_from"))
        & (F.col("r.read_at") < F.col("tw.valid_to")),
        "inner",
    )
    .select(
        "r.id",
        "r.site_id",
        "r.read_at",
        "r.kwh",
        "r.quality",
        "st.zone",
        "tw.rate",
        F.round(F.col("r.kwh") * F.col("tw.rate"), 2).alias("energy_cost"),
    )
)

ranked = (
    joined_ranges
    .select(
        "id",
        "site_id",
        "kwh",
        "quality",
        "zone",
        "rate",
        "energy_cost",
        F.min("kwh").over(w_site_id).alias("site_id_min_kwh"),
        F.lead("read_at", 3).over(w_read_at_year_site_id_by_rate).alias("next3_read_at"),
        F.nth_value("rate", 2).over(w_read_at_year_site_id_by_rate_2).alias("second_rate"),
        F.round(F.cume_dist().over(w_read_at_year_site_id_by_rate_3), 3).alias("rate_cume_dist"),
        F.count(F.lit(1)).over(w_site_id).alias("rows_in_site_id"),
        F.dense_rank().over(w_read_at_year_site_id_by_rate_3).alias("rate_dense_rank"),
    )
)

result = (
    ranked
    .filter(F.col("rows_in_site_id") >= 6)
    .select(
        "id",
        "kwh",
        "quality",
        "rate",
        "energy_cost",
        "site_id_min_kwh",
        "next3_read_at",
        "second_rate",
        "rate_cume_dist",
        "rows_in_site_id",
        "rate_dense_rank",
        F.round(F.col("rate") / F.nullif(F.col("site_id_min_kwh"), F.lit(0)), 3).alias("rate_per_site_id_min_kwh"),
    )
    .orderBy(F.col("id").desc(), F.col("second_rate").desc())
)
