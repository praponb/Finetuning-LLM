from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_total_rate_full = Window.orderBy(F.col("total_rate").asc_nulls_last(), "zone", "site_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_n_rows_by_total_rate_prior_6 = Window.partitionBy("n_rows").orderBy(F.col("total_rate").desc(), "zone", "site_id").rowsBetween(-6, -1)
w_n_rows_zone = Window.partitionBy("n_rows", "zone")
w_n_rows_by_total_rate = Window.partitionBy("n_rows").orderBy(F.col("total_rate").desc(), "zone", "site_id")
w_site_id_by_total_rate = Window.partitionBy("site_id").orderBy("total_rate", "zone", "site_id")

matched = (
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

totals = (
    matched
    .groupBy("zone", "site_id")
    .agg(
        F.percentile_approx("kwh", 0.5).alias("median_kwh"),
        F.sum("rate").alias("total_rate"),
        F.min("read_at").alias("first_read_at"),
        F.count("*").alias("n_rows"),
        F.round(F.sum("rate"), 2).alias("total_rate_2"),
    )
    .filter(F.col("n_rows") >= 2)
)

ranked = (
    totals
    .select(
        "zone",
        "site_id",
        "first_read_at",
        "total_rate_2",
        F.last("median_kwh").over(w_all_by_total_rate_full).alias("last_median_kwh"),
        F.round(F.avg("total_rate_2").over(w_n_rows_by_total_rate_prior_6), 2).alias("total_rate_2_avg_6_prior"),
        F.round(F.col("total_rate_2") / F.nullif(F.sum("total_rate_2").over(w_n_rows_zone), F.lit(0)), 4).alias("total_rate_2_share_of_n_rows_zone"),
        F.ntile(10).over(w_n_rows_by_total_rate).alias("total_rate_decile"),
        F.lag("median_kwh", 2).over(w_site_id_by_total_rate).alias("prev2_median_kwh"),
    )
)

detail_rows = (
    ranked.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "inner")
    .select(
        "s.zone",
        "s.site_id",
        "s.first_read_at",
        "s.last_median_kwh",
        "s.prev2_median_kwh",
        F.col("m.id").alias("reading_id"),
        F.col("m.kwh").alias("reading_kwh"),
        F.col("m.voltage").alias("reading_voltage"),
    )
)

result = (
    detail_rows
    .select(
        "zone",
        "site_id",
        "reading_voltage",
        "reading_kwh",
        "last_median_kwh",
    )
    .unpivot(
        ids=["zone", "site_id"],
        values=["reading_voltage", "reading_kwh", "last_median_kwh"],
        variableColumnName="metric",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
