from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_tariff_or_unknown = Window.partitionBy("tariff_or_unknown")
w_n_tariff_tou_by_cnt = Window.partitionBy("n_tariff_tou").orderBy(F.col("cnt").desc(), "tariff_or_unknown")
w_tariff_or_unknown_by_n_tarif = Window.partitionBy("tariff_or_unknown").orderBy(F.col("n_tariff_tou").desc(), "tariff_or_unknown")

valid = (
    df_sites
    .filter((F.year(F.col("commissioned_on")) == 2024) & (~F.col("tariff").isin("FLAT", "EV")))
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "installed_kw",
        "commissioned_on",
        F.coalesce(F.col("tariff"), F.lit("UNKNOWN")).alias("tariff_or_unknown"),
        F.add_months(F.col("commissioned_on"), 6).alias("commissioned_on_plus_6m"),
        F.date_trunc("week", F.col("commissioned_on")).alias("commissioned_on_week"),
    )
)

grouped = (
    valid
    .groupBy("tariff_or_unknown")
    .agg(
        F.count(F.when(F.col("tariff") == "TOU", 1)).alias("n_tariff_tou"),
        F.count("*").alias("cnt"),
    )
)

with_trends = (
    grouped
    .select(
        "tariff_or_unknown",
        "n_tariff_tou",
        "cnt",
        F.round(F.col("cnt") / F.nullif(F.sum("cnt").over(w_tariff_or_unknown), F.lit(0)), 4).alias("cnt_share_of_tariff_or_unknown"),
        F.round(F.cume_dist().over(w_n_tariff_tou_by_cnt), 3).alias("cnt_cume_dist"),
        F.row_number().over(w_tariff_or_unknown_by_n_tarif).alias("seq_no"),
    )
)

result = (
    with_trends
    .select(
        "tariff_or_unknown",
        "n_tariff_tou",
        "cnt",
        "cnt_share_of_tariff_or_unknown",
        "cnt_cume_dist",
    )
)
