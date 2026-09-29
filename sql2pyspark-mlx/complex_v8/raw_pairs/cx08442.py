from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_tariff_valid_to_to_date = Window.partitionBy("tariff", F.to_date("valid_to"))
w_valid_to_month_tariff_by_val = Window.partitionBy(F.month("valid_to"), "tariff").orderBy(F.col("valid_to").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_valid_to_month_tariff_by_val_2 = Window.partitionBy(F.month("valid_to"), "tariff").orderBy(F.col("valid_to").desc(), "id")

cleaned_tariffs = (
    df_tariff_windows
    .filter(F.col("tariff").isin("FLAT", "EV"))
    .select(
        "id",
        "tariff",
        "valid_to",
        "rate",
    )
)

windowed = (
    cleaned_tariffs
    .select(
        "*",
        F.sum("rate").over(w_tariff_valid_to_to_date).alias("tariff_valid_to_to_date_sum_rate"),
        F.min("rate").over(w_valid_to_month_tariff_by_val).alias("min_rate_so_far"),
        F.round(F.avg("rate").over(w_tariff_valid_to_to_date), 2).alias("tariff_valid_to_to_date_avg_rate"),
        F.count(F.lit(1)).over(w_tariff_valid_to_to_date).alias("rows_in_tariff_valid_to_to_date"),
        F.ntile(4).over(w_valid_to_month_tariff_by_val_2).alias("valid_to_quartile"),
        F.max("rate").over(w_tariff_valid_to_to_date).alias("tariff_valid_to_to_date_max_rate"),
    )
)

per_group = (
    windowed
    .groupBy("tariff")
    .agg(
        F.round(F.sum("tariff_valid_to_to_date_max_rate") / F.nullif(F.sum("rows_in_tariff_valid_to_to_date"), F.lit(0)), 3).alias("tariff_valid_to_to_date_max_rate_to_rows_in_tari"),
        F.min("valid_to").alias("first_valid_to"),
        F.avg("rows_in_tariff_valid_to_to_date").alias("avg_rows_in_tariff_valid_to_to_date_check"),
    )
    .filter(F.col("avg_rows_in_tariff_valid_to_to_date_check") > 4)
    .drop("avg_rows_in_tariff_valid_to_to_date_check")
)

enriched = (
    per_group
    .select(
        "*",
        F.date_format(F.col("first_valid_to"), "yyyy-MM-dd").alias("first_valid_to_label"),
        F.lower(F.regexp_replace(F.trim(F.col("tariff")), " ", "_")).alias("tariff_slug"),
        F.coalesce(F.col("tariff"), F.lit("UNKNOWN")).alias("tariff_or_unknown"),
    )
)

tariff_or_unknown_stats = (
    enriched
    .groupBy("tariff_or_unknown")
    .agg(F.percentile_approx("tariff_valid_to_to_date_max_rate_to_rows_in_tari", 0.5).alias("median_tariff_valid_to_to_date_max_rate_to_rows_"))
)

outliers = (
    enriched.alias("s")
    .join(tariff_or_unknown_stats.alias("g"), F.col("g.tariff_or_unknown") == F.col("s.tariff_or_unknown"), "inner")
    .filter(F.col("s.tariff_valid_to_to_date_max_rate_to_rows_in_tari") >= F.col("g.median_tariff_valid_to_to_date_max_rate_to_rows_"))
    .select(
        "s.*",
        "g.median_tariff_valid_to_to_date_max_rate_to_rows_",
        F.round(F.col("s.tariff_valid_to_to_date_max_rate_to_rows_in_tari") - F.col("g.median_tariff_valid_to_to_date_max_rate_to_rows_"), 2).alias("tariff_valid_to_to_date_max_rate_to_rows_in_tari_2"),
    )
)

result = (
    outliers
    .select(
        "tariff",
        "tariff_valid_to_to_date_max_rate_to_rows_in_tari",
        "first_valid_to",
        "first_valid_to_label",
        "tariff_slug",
        "tariff_or_unknown",
        "tariff_valid_to_to_date_max_rate_to_rows_in_tari_2",
        F.date_trunc("week", F.col("first_valid_to")).alias("first_valid_to_week"),
        F.lower(F.regexp_replace(F.trim(F.col("tariff_or_unknown")), " ", "_")).alias("tariff_or_unknown_slug"),
    )
)
