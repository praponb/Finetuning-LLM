from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_n_tariff_not_in_tou_ev_by_va = Window.partitionBy("n_tariff_not_in_tou_ev").orderBy("valid_from_day_month", "valid_from_day", "tariff", "valid_to_month").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_n_tariff_not_in_tou_ev_by_va_2 = Window.partitionBy("n_tariff_not_in_tou_ev").orderBy("valid_from_day_month", "valid_from_day", "tariff", "valid_to_month")
w_valid_to_month_year = Window.partitionBy(F.year("valid_to_month"))

scoped = (
    df_tariff_windows
    .filter(F.col("rate") > 3.0)
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
        F.lower(F.regexp_replace(F.trim(F.col("tariff")), " ", "_")).alias("tariff_slug"),
    )
)

summary = (
    scoped
    .groupBy(
        F.to_date("valid_from").alias("valid_from_day"),
        "tariff",
        F.date_trunc("month", "valid_to").alias("valid_to_month"),
    )
    .agg(
        F.round(100 * F.count(F.when(F.col("tariff") == "EV", 1)) / F.count("*"), 1).alias("pct_tariff_ev"),
        F.max("valid_from").alias("last_valid_from"),
        F.count(F.when(~F.col("tariff").isin("TOU", "EV"), 1)).alias("n_tariff_not_in_tou_ev"),
        F.round(100 * F.count(F.when(F.col("tariff") == "TOU", 1)) / F.count("*"), 1).alias("pct_tariff_tou"),
    )
)

scored = summary.withColumn("valid_from_day_month", F.date_trunc("month", F.col("valid_from_day")))

without_matches = (
    scored
    .crossJoin(scored.agg(F.avg("pct_tariff_ev").alias("overall_avg_pct_tariff_ev")))
    .filter(F.col("pct_tariff_ev") > F.col("overall_avg_pct_tariff_ev"))
    .drop("overall_avg_pct_tariff_ev")
)

sequenced = (
    without_matches
    .select(
        "*",
        F.count(F.lit(1)).over(w_n_tariff_not_in_tou_ev_by_va).alias("running_count"),
        F.ntile(10).over(w_n_tariff_not_in_tou_ev_by_va_2).alias("valid_from_day_month_decile"),
        F.ntile(3).over(w_n_tariff_not_in_tou_ev_by_va_2).alias("valid_from_day_month_tercile"),
        F.round(F.col("pct_tariff_ev") - F.avg("pct_tariff_ev").over(w_valid_to_month_year), 2).alias("pct_tariff_ev_vs_valid_to_month_year_avg"),
    )
)

enriched = (
    sequenced
    .select(
        "valid_from_day",
        "tariff",
        "valid_to_month",
        "n_tariff_not_in_tou_ev",
        "pct_tariff_tou",
        "valid_from_day_month",
        "running_count",
        "valid_from_day_month_tercile",
        "pct_tariff_ev_vs_valid_to_month_year_avg",
        F.last_day(F.col("valid_from_day")).alias("valid_from_day_month_end"),
    )
)

result = (
    enriched
    .groupBy("valid_from_day", "valid_from_day_month_tercile")
    .pivot("tariff", ["EV", "FLAT", "TOU"])
    .agg(F.count("running_count"))
    .orderBy("valid_from_day", "valid_from_day_month_tercile")
)
