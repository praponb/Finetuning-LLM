from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_month_site_id = Window.partitionBy(F.date_trunc("month", "read_at"), "site_id")
w_read_at_quarter_month_by_rea = Window.partitionBy(F.month("read_at_quarter")).orderBy(F.col("read_at_quarter").desc(), "id")
w_read_at_quarter_dense_rank = Window.partitionBy("read_at_quarter_dense_rank")
w_all_by_median_kwh = Window.orderBy("median_kwh", "id")
w_all_by_next2_kwh_minus_media = Window.orderBy("next2_kwh_minus_median", "id")
w_all_by_median_kwh_prior_2 = Window.orderBy("median_kwh", "id").rowsBetween(-2, -1)

prepared = (
    df_meter_readings
    .filter((F.col("read_at") >= F.lit("2025-01-19 09:00:00").cast("timestamp")) & (F.col("kwh") > 43.1))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.date_trunc("quarter", F.col("read_at")).alias("read_at_quarter"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("read_at")).alias("days_since_read_at"),
    )
)

site_id_stats = (
    prepared
    .groupBy("site_id")
    .agg(F.percentile_approx("kwh", 0.5).alias("median_kwh"))
)

compared = (
    prepared.alias("s")
    .join(site_id_stats.alias("g"), F.col("g.site_id") == F.col("s.site_id"), "inner")
    .filter(F.col("s.kwh") >= F.col("g.median_kwh"))
    .select(
        "s.*",
        "g.median_kwh",
        F.round(F.col("s.kwh") - F.col("g.median_kwh"), 2).alias("kwh_minus_median"),
    )
)

ranked = (
    compared
    .select(
        "*",
        F.dense_rank().over(w_read_at_quarter_month_by_rea).alias("read_at_quarter_dense_rank"),
        F.count(F.lit(1)).over(w_read_at_month_site_id).alias("rows_in_read_at_month_site_id"),
        F.lead("kwh_minus_median", 2).over(w_read_at_quarter_month_by_rea).alias("next2_kwh_minus_median"),
    )
)

metrics = (
    ranked
    .select(
        "*",
        F.sum("days_since_read_at").over(w_read_at_quarter_dense_rank).alias("read_at_quarter_dense_rank_sum_days_since_read_a"),
        F.round(F.col("voltage") - F.avg("voltage").over(w_read_at_quarter_dense_rank), 2).alias("voltage_vs_read_at_quarter_dense_rank_avg"),
        (F.col("voltage") - F.lag("voltage").over(w_all_by_median_kwh)).alias("voltage_change"),
        F.min("voltage").over(w_read_at_quarter_dense_rank).alias("read_at_quarter_dense_rank_min_voltage"),
        F.ntile(10).over(w_all_by_next2_kwh_minus_media).alias("next2_kwh_minus_median_decile"),
        F.sum("median_kwh").over(w_all_by_median_kwh_prior_2).alias("median_kwh_sum_2_prior"),
    )
)

result = (
    metrics
    .select(
        "id",
        "rows_in_read_at_month_site_id",
        "next2_kwh_minus_median_decile",
        "days_since_read_at",
        "read_at_quarter_dense_rank_sum_days_since_read_a",
    )
    .unpivot(
        ids=["id"],
        values=["rows_in_read_at_month_site_id", "next2_kwh_minus_median_decile", "days_since_read_at", "read_at_quarter_dense_rank_sum_days_since_read_a"],
        variableColumnName="measure",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
    .orderBy("id", "measure")
)
